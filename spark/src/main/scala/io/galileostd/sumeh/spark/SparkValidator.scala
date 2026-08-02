package io.galileostd.sumeh.spark

import java.util.UUID

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.{ DoubleValue, ListValue, LongValue, RuleDefinition, RuleValue, StringValue }
import io.galileostd.sumeh.spark.expr.FailCondition
import io.galileostd.sumeh.spark.registry.SparkRegistry
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationReport, ValidationResult, ValidationStatus }
import org.apache.spark.sql.{ functions => F, Column, DataFrame, Row }
import org.apache.spark.sql.types.{ ArrayType, StringType, StructField, StructType }

/**
 * Spark entry point: validates DataFrames using the Bifurcation Pattern.
 *
 * Works on batch AND streaming DataFrames (auto-detected via `df.isStreaming`). Rules that need state (uniqueness),
 * custom SQL, or TABLE-level aggregation are skipped with a reason on streaming input — never silently passed.
 *
 * Batch: ROW rules are evaluated and failing rows annotated in a `_dq_errors` column in a single pass; TABLE rules run
 * as aggregations. Streaming: the same `_dq_errors` annotation is applied purely with column expressions (no
 * aggregation, no `.collect()`), mirroring the Flink engine, and skipped rules are surfaced in a `_dq_skipped` column.
 */
object SparkValidator {

  /**
   * Schema of the `_dq_errors` struct attached to each validated row.
   *
   * One struct entry per failing rule. Field names and order match the Python implementation so cross-language
   * consumers see the same contract.
   */
  private val errorSchema = ArrayType(
    StructType(
      Seq(
        StructField("rule_id", StringType, nullable = true),
        StructField("check_type", StringType, nullable = true),
        StructField("field", StringType, nullable = true),
        StructField("category", StringType, nullable = true),
        StructField("expected", StringType, nullable = true),
        StructField("actual", StringType, nullable = true),
        StructField("message", StringType, nullable = true),
        StructField("timestamp", StringType, nullable = true)
      )
    )
  )

  /**
   * Metric kind per `check_type`, mirroring the values the per-rule analyzers produced before the single-pass rewrite.
   *
   * Leaks into the user-facing report via `ValidationResult.metadata`/`metricType`, so it must not invent new names.
   */
  private val metricTypeFor: Map[String, String] = Map(
    // Completeness
    "is_complete"  -> "completeness",
    "are_complete" -> "multi_field_completeness",
    // Comparison
    "is_equal"                 -> "comparison",
    "is_equal_than"            -> "column_comparison",
    "is_between"               -> "between",
    "is_greater_than"          -> "comparison",
    "is_less_than"             -> "comparison",
    "is_greater_or_equal_than" -> "comparison",
    "is_less_or_equal_than"    -> "comparison",
    "is_positive"              -> "comparison",
    "is_negative"              -> "comparison",
    "is_in_millions"           -> "comparison",
    "is_in_billions"           -> "comparison",
    // Membership
    "is_contained_in"  -> "membership",
    "not_contained_in" -> "membership",
    "is_in"            -> "membership",
    "not_in"           -> "membership",
    // Pattern
    "has_pattern" -> "pattern",
    "is_legit"    -> "legit",
    // Date
    "is_today"             -> "date",
    "is_t_minus_1"         -> "date",
    "is_t_minus_2"         -> "date",
    "is_t_minus_3"         -> "date",
    "is_yesterday"         -> "date",
    "is_past_date"         -> "date",
    "is_future_date"       -> "date",
    "is_date_between"      -> "date_between",
    "is_date_after"        -> "date_comparison",
    "is_date_before"       -> "date_comparison",
    "is_on_weekday"        -> "date",
    "is_on_weekend"        -> "date",
    "is_on_monday"         -> "date",
    "is_on_tuesday"        -> "date",
    "is_on_wednesday"      -> "date",
    "is_on_thursday"       -> "date",
    "is_on_friday"         -> "date",
    "is_on_saturday"       -> "date",
    "is_on_sunday"         -> "date",
    "validate_date_format" -> "date_format",
    "all_date_checks"      -> "date",
    // SQL
    "satisfies" -> "satisfies"
  )

  /**
   * Validates a Spark DataFrame using the Bifurcation Pattern.
   *
   * Single-pass: adds a `_dq_errors` column per row. Use `report.split()` to separate good/bad rows. Row-level data is
   * never `.collect()`ed.
   *
   * On a streaming DataFrame the validation runs as a pure column-expression transformation — no aggregation, no
   * `.collect()` — mirroring the Flink engine. Rules that need state (uniqueness), custom SQL, or TABLE-level
   * aggregation are SKIPPED with a reason (surfaced in the `_dq_skipped` column and `report.results`), and evaluated
   * row rules carry no in-stream verdict.
   *
   * Note: `threshold` only affects the status of each [[io.galileostd.sumeh.validation.ValidationResult]]. Rows that
   * violate a rule are always marked in `_dq_errors`, even when the rule passes the threshold. A report with pass rate
   * 1.0 can still have rows in the `bad` DataFrame.
   *
   * Note: the order of `report.results` is NOT the input rule order. In batch it is simple ROW rules (in input order),
   * then uniqueness rules, then TABLE rules — do not rely on `rules.zip(report.results)`.
   *
   * Note: `_dq_errors` is an `array<struct<rule_id, check_type, field, category, expected, actual, message,
   * timestamp>>` Spark, but a JSON string carrying the same fields in the Flink engine. `_dq_skipped` is a
   * `checkType:reason` string with `|` separators in both engines. Cross-engine sinks must handle the two `_dq_errors`
   * shapes.
   *
   * @param df The DataFrame to validate (batch or streaming).
   * @param rules The rules to run.
   * @return a [[ValidationReport]] with pass/fail per rule, ready for `.split()`
   */
  def validate(
      df: DataFrame,
      rules: Seq[RuleDefinition]
  ): ValidationReport[ValidatedSparkDataFrame] =
    if (df.isStreaming) validateStreaming(df, rules)
    else validateBatch(df, rules)

  // -------------------------------------------------------------------------
  // Batch path — single aggregation, single annotation, analyzers only for
  // uniqueness and TABLE rules
  // -------------------------------------------------------------------------

  /**
   * Batch validation path.
   *
   * A constant number of passes regardless of how many ROW rules exist: (1) one `agg` computes every simple rule's fail
   * count, (2) one `withColumn` builds `_dq_errors` for all failing rules, (3) uniqueness rules keep their own
   * `groupBy` analyzers and TABLE rules run as aggregations. The annotation runs over the *original* `df`, never over a
   * re-annotated intermediate, so rule N+1 does not re-evaluate rule N's expressions.
   *
   * A rule whose `FailCondition` throws or whose field is missing becomes an ERROR result and is excluded from the
   * shared agg — it never aborts the batch.
   *
   * @param df The DataFrame to validate.
   * @param rules The rules to run.
   * @return a [[ValidationReport]] whose `dfValidated` field holds the annotated DataFrame
   */
  private def validateBatch(
      df: DataFrame,
      rules: Seq[RuleDefinition]
  ): ValidationReport[ValidatedSparkDataFrame] = {

    val startTime = System.currentTimeMillis()

    val rowRules   = rules.filter(_.isApplicableForLevel("ROW"))
    val tableRules = rules.filter(_.isApplicableForLevel("TABLE"))

    val results        = scala.collection.mutable.ListBuffer[ValidationResult]()
    val skippedReasons = scala.collection.mutable.ListBuffer[String]()

    // Uniqueness needs a windowed/groupBy fail condition, which cannot nest inside a shared
    // aggregation — it keeps its own analyzer per rule. Everything else shares one agg.
    val (uniquenessRules, simpleRowRules) = rowRules.partition {
      r => Set("is_unique", "are_unique", "is_primary_key", "is_composite_key").contains(r.checkType)
    }

    // -------------------------------------------------------------------------
    // ROW-LEVEL (simple): build fail counters, one per rule, tolerating broken rules
    // -------------------------------------------------------------------------

    // Right(i) = the rule's fail count lives at column index i of the shared agg;
    // Left = immediate skipped/error result.
    val outcomes    = scala.collection.mutable.ListBuffer[Either[ValidationResult, Int]]()
    val counterCols = scala.collection.mutable.ListBuffer[Column]()
    val executable  = scala.collection.mutable.ListBuffer[RuleDefinition]()

    for (rule <- simpleRowRules)
      rule.skipReason("ROW", "spark") match {
        case Some(reason) =>
          outcomes += Left(skippedResult(rule, ValidationLevel.ROW, reason))
          skippedReasons += s"${rule.checkType}:$reason"

        case None =>
          try {
            validateFields(df, rule)
            val failCond = FailCondition(rule)
            counterCols += F.sum(F.when(failCond, 1L).otherwise(0L))
            executable += rule
            outcomes += Right(executable.size - 1)
          } catch {
            case e: Exception =>
              outcomes += Left(errorResult(rule, ValidationLevel.ROW, e.getMessage))
          }
      }

    // One aggregation over the ORIGINAL df — never over an annotated intermediate.
    val aggRow: Option[Row] =
      if (counterCols.isEmpty) None
      else {
        val cols  = counterCols.zipWithIndex.map { case (col, i) => col.alias(s"_fail_$i") }
        val total = F.count(F.lit(1)).alias("_total")
        Some(df.agg(total, cols.toSeq: _*).collect()(0))
      }

    val totalRows = aggRow.map(_.getAs[Long]("_total")).getOrElse(df.count())

    // Metrics + constraints for the executable simple rules.
    val simpleResults = executable.zipWithIndex.map {
      case (rule, i) =>
        val failCount = aggRow
          .map {
            r =>
              val idx = r.fieldIndex(s"_fail_$i")
              if (r.isNullAt(idx)) 0L else r.getLong(idx)
          }
          .getOrElse(0L)
        val metric = MetricResult(
          metricType = metricTypeFor.getOrElse(rule.checkType, rule.checkType),
          field = rule.field,
          value = if (totalRows > 0) (totalRows - failCount).toDouble / totalRows else 1.0,
          totalRows = totalRows,
          metadata = buildMetricMetadata(rule, failCount, totalRows)
        )
        SparkRegistry.getConstraint(rule.checkType).check(metric, rule)
    }

    results ++= outcomes.map {
      case Left(r)  => r
      case Right(i) => simpleResults(i)
    }

    // -------------------------------------------------------------------------
    // Annotate _dq_errors in a single withColumn over the original df
    // -------------------------------------------------------------------------

    // Every executed ROW rule marks its violating rows — threshold only governs
    // the report status, never which rows land in `_dq_errors`.
    val markedRules  = executable.zip(simpleResults)
    val errorEntries = markedRules.map { case (rule, result) => F.when(FailCondition(rule), errorStruct(rule, result)) }

    var workDf =
      if (errorEntries.isEmpty) df.withColumn("_dq_errors", F.array().cast(errorSchema))
      else df.withColumn("_dq_errors", F.array_compact(F.array(errorEntries.toSeq: _*)).cast(errorSchema))

    // -------------------------------------------------------------------------
    // ROW-LEVEL (uniqueness): individual analyzers, incremental annotation
    // -------------------------------------------------------------------------
    for (rule <- uniquenessRules)
      rule.skipReason("ROW", "spark") match {
        case Some(reason) =>
          results += skippedResult(rule, ValidationLevel.ROW, reason)
          skippedReasons += s"${rule.checkType}:$reason"

        case None =>
          try {
            val analyzer   = SparkRegistry.getAnalyzer(rule.checkType)
            val constraint = SparkRegistry.getConstraint(rule.checkType)
            val metric     = analyzer.analyze(df, rule)
            val result     = constraint.check(metric, rule)
            results += result

            val failCond = FailCondition(rule)
            workDf = workDf.withColumn(
              "_dq_errors",
              F.when(failCond, F.array_union(F.col("_dq_errors"), F.array(errorStruct(rule, result))))
                .otherwise(F.col("_dq_errors"))
            )
          } catch {
            case e: Exception =>
              results += errorResult(rule, ValidationLevel.ROW, e.getMessage)
          }
      }

    // -------------------------------------------------------------------------
    // TABLE-LEVEL: aggregations, no _dq_errors annotation needed
    // -------------------------------------------------------------------------
    for (rule <- tableRules)
      rule.skipReason("TABLE", "spark") match {
        case Some(reason) =>
          results += skippedResult(rule, ValidationLevel.TABLE, reason)
          skippedReasons += s"${rule.checkType}:$reason"

        case None =>
          try {
            val analyzer   = SparkRegistry.getAnalyzer(rule.checkType)
            val constraint = SparkRegistry.getConstraint(rule.checkType)
            val metric     = analyzer.analyze(df, rule)
            results += constraint.check(metric, rule)
          } catch {
            case e: Exception =>
              results += errorResult(rule, ValidationLevel.TABLE, e.getMessage)
          }
      }

    val annotated       = workDf.withColumn("_dq_skipped", F.lit(skippedReasons.mkString("|")))
    val executionTimeMs = (System.currentTimeMillis() - startTime).toDouble
    val validated       = new ValidatedSparkDataFrame(annotated)

    ValidationReport(
      results = results.toList,
      totalRows = totalRows,
      executionTimeMs = executionTimeMs,
      engine = "spark",
      dfValidated = Some(validated)
    )
  }

  // -------------------------------------------------------------------------
  // Streaming path — column-expression annotation only, no eager ops
  // -------------------------------------------------------------------------

  /**
   * Streaming validation path.
   *
   * ROW rules are applied as pure column expressions: each failing row gets an error struct appended to `_dq_errors`,
   * and no rule carries a runtime verdict (there is no finite aggregation on a stream). TABLE rules are always skipped
   * on a stream, and all skip reasons are joined into a `_dq_skipped` column.
   *
   * @param df The streaming DataFrame to validate.
   * @param rules The rules to run.
   * @return a [[ValidationReport]] with `totalRows=-1` and `engine="spark-streaming"`; `.split()` works
   */
  private def validateStreaming(
      df: DataFrame,
      rules: Seq[RuleDefinition]
  ): ValidationReport[ValidatedSparkDataFrame] = {

    val startTime = System.currentTimeMillis()

    val rowRules   = rules.filter(_.isApplicableForLevel("ROW"))
    val tableRules = rules.filter(_.isApplicableForLevel("TABLE"))

    val results        = scala.collection.mutable.ListBuffer[ValidationResult]()
    val skippedReasons = scala.collection.mutable.ListBuffer[String]()
    val errorEntries   = scala.collection.mutable.ListBuffer[Column]()

    // ROW-LEVEL: build every rule's error entry in one pass (no aggregation)
    for (rule <- rowRules)
      rule.skipReason("ROW", "spark-streaming") match {
        case Some(reason) =>
          results += skippedResult(rule, ValidationLevel.ROW, reason)
          skippedReasons += s"${rule.checkType}:$reason"

        case None =>
          try {
            val sResult = streamingResult(rule)
            errorEntries += F.when(FailCondition(rule), errorStruct(rule, sResult))
            results += sResult
          } catch {
            case e: Exception =>
              results += errorResult(rule, ValidationLevel.ROW, e.getMessage)
          }
      }

    // TABLE-LEVEL: aggregations are unsupported on a stream — always skipped
    for (rule <- tableRules)
      rule.skipReason("TABLE", "spark-streaming") match {
        case Some(reason) =>
          results += skippedResult(rule, ValidationLevel.TABLE, reason)
          skippedReasons += s"${rule.checkType}:$reason"

        case None =>
          results += errorResult(rule, ValidationLevel.TABLE, "TABLE-level rule executed on a streaming DataFrame")
      }

    val workDf =
      if (errorEntries.isEmpty) df.withColumn("_dq_errors", F.array().cast(errorSchema))
      else df.withColumn("_dq_errors", F.array_compact(F.array(errorEntries.toSeq: _*)).cast(errorSchema))

    val annotated = workDf.withColumn("_dq_skipped", F.lit(skippedReasons.mkString("|")))

    ValidationReport(
      results = results.toList,
      totalRows = -1L, // unbounded stream: row count is unknown
      executionTimeMs = (System.currentTimeMillis() - startTime).toDouble,
      engine = "spark-streaming",
      dfValidated = Some(new ValidatedSparkDataFrame(annotated))
    )
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Validates that every column a rule's fail condition references exists in the DataFrame.
   *
   * Replaces the `requireField` calls that lived inside the analyzers, which the single-pass batch path no longer
   * invokes. `satisfies` references arbitrary SQL, so its `field` is not checked (matching the analyzer's behavior).
   *
   * @param df The DataFrame being validated.
   * @param rule the [[RuleDefinition]] being validated
   * @throws java.lang.IllegalArgumentException when a referenced column is absent.
   */
  private def validateFields(df: DataFrame, rule: RuleDefinition): Unit = {
    if (rule.checkType != "satisfies")
      rule.field.fold(f => requireField(df, f), fields => fields.foreach(requireField(df, _)))

    rule.checkType match {
      case "is_equal_than" =>
        val other = FailCondition.requireString(rule, "is_equal_than requires a column name as value")
        requireField(df, other)
      case _ =>
    }
  }

  /**
   * Throws if the field is not a column of the DataFrame.
   *
   * @param df The DataFrame.
   * @param field The column name to check.
   * @throws java.lang.IllegalArgumentException when the field is absent.
   */
  private def requireField(df: DataFrame, field: String): Unit =
    if (!df.columns.exists(_.equalsIgnoreCase(field)))
      throw new IllegalArgumentException(s"Field '$field' not found in DataFrame")

  /**
   * Rebuilds the metadata an analyzer used to attach to its metric, preserving the exact keys the single-pass rewrite
   * replaces. Keeps the user-facing report stable.
   *
   * @param rule The rule.
   * @param failCount Number of failing rows (from the shared agg).
   * @param totalRows Total rows.
   * @return The metadata map, mirroring the original analyzer output.
   */
  private def buildMetricMetadata(rule: RuleDefinition, failCount: Long, totalRows: Long): Map[String, Any] =
    rule.checkType match {
      case "is_complete" =>
        Map("null_count" -> failCount, "total_count" -> totalRows)

      case "are_complete" =>
        Map("incomplete_count" -> failCount, "total_count" -> totalRows, "fields" -> rule.field.fold(List(_), identity))

      case "is_equal" | "is_greater_than" | "is_less_than" | "is_greater_or_equal_than" | "is_less_or_equal_than" |
          "is_positive" | "is_negative" | "is_in_millions" | "is_in_billions" =>
        Map(
          "fail_count"  -> failCount,
          "total_count" -> totalRows,
          "threshold"   -> rule.value.map(RuleValue.toAny).orNull
        )

      case "is_between" =>
        val (lo, hi) = FailCondition.requirePair(rule, "is_between requires value=[min, max]")
        Map("fail_count" -> failCount, "min" -> RuleValue.toAny(lo), "max" -> RuleValue.toAny(hi))

      case "is_equal_than" =>
        val other = FailCondition.requireString(rule, "is_equal_than requires a column name as value")
        Map("fail_count" -> failCount, "compared_to" -> other)

      case "is_contained_in" | "not_contained_in" | "is_in" | "not_in" =>
        val values = FailCondition.requireList(rule, "Membership requires a list of values").map {
          case StringValue(s) => s
          case LongValue(l)   => l
          case DoubleValue(d) => d
          case other          => other.toString
        }
        Map("fail_count" -> failCount, "values" -> values)

      case "has_pattern" =>
        val pattern = FailCondition.requireString(rule, "has_pattern requires a regex pattern")
        Map("fail_count" -> failCount, "pattern" -> pattern)

      case "is_legit" =>
        Map("fail_count" -> failCount)

      case "is_today" | "is_t_minus_1" | "is_t_minus_2" | "is_t_minus_3" | "is_yesterday" | "is_past_date" |
          "is_future_date" | "is_on_weekday" | "is_on_weekend" | "is_on_monday" | "is_on_tuesday" | "is_on_wednesday" |
          "is_on_thursday" | "is_on_friday" | "is_on_saturday" | "is_on_sunday" | "all_date_checks" =>
        Map("fail_count" -> failCount, "check_type" -> rule.checkType)

      case "is_date_between" =>
        val (start, end) = FailCondition.requirePair(rule, "is_date_between requires value=[start, end]") match {
          case (StringValue(s), StringValue(e)) => (s, e)
          case _ =>
            throw new IllegalArgumentException("is_date_between requires [start, end] date strings")
        }
        Map("fail_count" -> failCount, "start" -> start, "end" -> end)

      case "is_date_after" | "is_date_before" =>
        val target = FailCondition.requireString(rule, s"${rule.checkType} requires a date value")
        Map("fail_count" -> failCount, "target" -> target)

      case "validate_date_format" =>
        val format = FailCondition.requireString(rule, "validate_date_format requires a format string as value")
        Map("fail_count" -> failCount, "format" -> format)

      case "satisfies" =>
        val condition = FailCondition.requireString(rule, "satisfies requires a SQL condition as value")
        Map("fail_count" -> failCount, "condition" -> condition)

      case other =>
        Map("fail_count" -> failCount, "total_count" -> totalRows)
    }

  /**
   * Builds the `_dq_errors` struct entry for a rule result.
   *
   * @param rule The rule.
   * @param result The constraint result carrying id/message/expected/actual.
   * @return A struct column matching `errorSchema`.
   */
  private def errorStruct(rule: RuleDefinition, result: ValidationResult): Column =
    F.struct(
      F.lit(result.id).cast(StringType).alias("rule_id"),
      F.lit(rule.checkType).cast(StringType).alias("check_type"),
      F.lit(rule.fieldName).cast(StringType).alias("field"),
      F.lit(rule.category).cast(StringType).alias("category"),
      F.lit(result.expectedValue.map(_.toString).orNull).cast(StringType).alias("expected"),
      F.lit(result.actualValue.map(_.toString).orNull).cast(StringType).alias("actual"),
      F.lit(result.message.orNull).cast(StringType).alias("message"),
      F.lit(result.timestamp.toString).cast(StringType).alias("timestamp")
    )

  /**
   * Builds a SKIPPED result for a rule.
   *
   * @param rule The rule.
   * @param level The level being evaluated.
   * @param reason Why the rule was skipped.
   * @return A SKIPPED [[io.galileostd.sumeh.validation.ValidationResult]].
   */
  private def skippedResult(rule: RuleDefinition, level: ValidationLevel, reason: String) =
    ValidationResult.skipped(
      checkType = rule.checkType,
      field = rule.field,
      level = level,
      category = rule.category,
      reason = reason
    )

  /**
   * Builds an ERROR result for a rule.
   *
   * @param rule The rule.
   * @param level The level being evaluated.
   * @param msg The error message.
   * @return An ERROR [[io.galileostd.sumeh.validation.ValidationResult]].
   */
  private def errorResult(rule: RuleDefinition, level: ValidationLevel, msg: String) =
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = level,
      category = rule.category,
      status = ValidationStatus.ERROR,
      message = Some(s"Error: $msg")
    )

  /**
   * Builds the result for a rule that was applied on a streaming DataFrame.
   *
   * A stream has no finite aggregation, so no pass rate is computed — the rule is reported as applied without a
   * `passRate`. This keeps `report.size` honest: every rule that ran appears in the report.
   *
   * @param rule The rule that was applied.
   * @return A PASS [[io.galileostd.sumeh.validation.ValidationResult]] with no pass rate.
   */
  private def streamingResult(rule: RuleDefinition) =
    ValidationResult(
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.ROW,
      category = rule.category,
      status = ValidationStatus.PASS,
      metadata = Map("note" -> "pass rate is not computable in streaming")
    )
}
