package io.galileostd.sumeh.spark

import java.util.UUID

import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.spark.expr.FailCondition
import io.galileostd.sumeh.spark.registry.SparkRegistry
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationReport, ValidationResult, ValidationStatus }
import org.apache.spark.sql.{ functions => F, DataFrame }
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
   * One struct entry per failing rule with `rule_id` and `check_type` fields.
   */
  private val errorSchema = ArrayType(
    StructType(
      Seq(
        StructField("rule_id", StringType, nullable = true),
        StructField("check_type", StringType, nullable = true),
        StructField("field", StringType, nullable = true),
        StructField("category", StringType, nullable = true),
        StructField("message", StringType, nullable = true),
        StructField("expected", StringType, nullable = true),
        StructField("actual", StringType, nullable = true)
      )
    )
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
   * Args: df: The DataFrame to validate (batch or streaming). rules: The rules to run.
   *
   * Returns: A report with per-rule results and the validated wrapper for splitting.
   */
  def validate(
      df: DataFrame,
      rules: Seq[RuleDefinition]
  ): ValidationReport[ValidatedSparkDataFrame] =
    if (df.isStreaming) validateStreaming(df, rules)
    else validateBatch(df, rules)

  // -------------------------------------------------------------------------
  // Batch path — analyzers compute metrics, TABLE rules run, full report
  // -------------------------------------------------------------------------

  /**
   * Batch validation path.
   *
   * ROW rules are evaluated per rule: the analyzer computes a metric, the constraint decides pass/fail, and failing
   * rows get an error struct appended to `_dq_errors` (reusing the analyzer logic as a column predicate, so the
   * annotation shares the same single pass). TABLE rules run as aggregations on the annotated DataFrame. Skips,
   * failures, and errors all land in the report.
   *
   * Args: df: The DataFrame to validate. rules: The rules to run.
   *
   * Returns: A complete report with the annotated DataFrame attached.
   */
  private def validateBatch(
      df: DataFrame,
      rules: Seq[RuleDefinition]
  ): ValidationReport[ValidatedSparkDataFrame] = {

    val startTime = System.currentTimeMillis()

    val rowRules   = rules.filter(_.isApplicableForLevel("ROW"))
    val tableRules = rules.filter(_.isApplicableForLevel("TABLE"))

    val results = scala.collection.mutable.ListBuffer[ValidationResult]()

    // Initialize _dq_errors column
    var workDf = df.withColumn("_dq_errors", F.array().cast(errorSchema))

    // -------------------------------------------------------------------------
    // ROW-LEVEL: run analyzers, build _dq_errors in single pass
    // -------------------------------------------------------------------------
    for (rule <- rowRules)
      rule.skipReason("ROW", "spark") match {
        case Some(reason) =>
          results += skippedResult(rule, ValidationLevel.ROW, reason)

        case None =>
          try {
            val analyzer   = SparkRegistry.getAnalyzer(rule.checkType)
            val constraint = SparkRegistry.getConstraint(rule.checkType)
            val metric     = analyzer.analyze(workDf, rule)
            val result     = constraint.check(metric, rule)
            results += result

            // Bifurcation: append error struct to failing rows
            if (result.status == ValidationStatus.FAIL) {
              val errorStruct = F.struct(
                F.lit(result.id).cast(StringType).alias("rule_id"),
                F.lit(rule.checkType).cast(StringType).alias("check_type"),
                F.lit(rule.fieldName).cast(StringType).alias("field"),
                F.lit(rule.category).cast(StringType).alias("category"),
                F.lit(result.message.orNull).cast(StringType).alias("message"),
                F.lit(result.expectedValue.map(_.toString).orNull).cast(StringType).alias("expected"),
                F.lit(result.actualValue.map(_.toString).orNull).cast(StringType).alias("actual")
              )

              // failCondition: reuse analyzer logic via column expression
              val failCond = FailCondition(rule)
              workDf = workDf.withColumn(
                "_dq_errors",
                F.when(
                  failCond,
                  F.array_union(F.col("_dq_errors"), F.array(errorStruct))
                ).otherwise(F.col("_dq_errors"))
              )
            }
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

        case None =>
          try {
            val analyzer   = SparkRegistry.getAnalyzer(rule.checkType)
            val constraint = SparkRegistry.getConstraint(rule.checkType)
            val metric     = analyzer.analyze(workDf, rule)
            results += constraint.check(metric, rule)
          } catch {
            case e: Exception =>
              results += errorResult(rule, ValidationLevel.TABLE, e.getMessage)
          }
      }

    val executionTimeMs = (System.currentTimeMillis() - startTime).toDouble
    val validated       = new ValidatedSparkDataFrame(workDf)

    ValidationReport(
      results = results.toList,
      totalRows = df.count(),
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
   * Args: df: The streaming DataFrame to validate. rules: The rules to run.
   *
   * Returns: A report (totalRows `-1L`, engine `"spark-streaming"`) with the annotated DataFrame attached.
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

    var workDf = df.withColumn("_dq_errors", F.array().cast(errorSchema))

    // ROW-LEVEL: annotate failing rows via column expressions (no aggregation)
    for (rule <- rowRules)
      rule.skipReason("ROW", "spark-streaming") match {
        case Some(reason) =>
          results += skippedResult(rule, ValidationLevel.ROW, reason)
          skippedReasons += s"${rule.checkType}:$reason"

        case None =>
          try {
            val errorStruct = F.struct(
              F.lit(UUID.randomUUID().toString).cast(StringType).alias("rule_id"),
              F.lit(rule.checkType).cast(StringType).alias("check_type"),
              F.lit(rule.fieldName).cast(StringType).alias("field"),
              F.lit(rule.category).cast(StringType).alias("category"),
              F.lit(null: String).cast(StringType).alias("message"),
              F.lit(rule.value.map(_.toString).orNull).cast(StringType).alias("expected"),
              F.lit(null: String).cast(StringType).alias("actual")
            )
            val failCond = FailCondition(rule)
            workDf = workDf.withColumn(
              "_dq_errors",
              F.when(failCond, F.array_union(F.col("_dq_errors"), F.array(errorStruct)))
                .otherwise(F.col("_dq_errors"))
            )
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

    workDf = workDf.withColumn("_dq_skipped", F.lit(skippedReasons.mkString("|")))

    ValidationReport(
      results = results.toList,
      totalRows = -1L, // unbounded stream: row count is unknown
      executionTimeMs = (System.currentTimeMillis() - startTime).toDouble,
      engine = "spark-streaming",
      dfValidated = Some(new ValidatedSparkDataFrame(workDf))
    )
  }

  /**
   * Builds a SKIPPED result for a rule.
   *
   * Args: rule: The rule. level: The level being evaluated. reason: Why the rule was skipped.
   *
   * Returns: A SKIPPED [[io.galileostd.sumeh.validation.ValidationResult]].
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
   * Args: rule: The rule. level: The level being evaluated. msg: The error message.
   *
   * Returns: An ERROR [[io.galileostd.sumeh.validation.ValidationResult]].
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
}
