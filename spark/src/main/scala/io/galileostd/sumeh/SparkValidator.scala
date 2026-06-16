package io.galileostd.sumeh.spark

import java.util.UUID

import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.spark.registry.SparkRegistry
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationReport, ValidationResult, ValidationStatus }
import org.apache.spark.sql.{ functions => F, DataFrame }
import org.apache.spark.sql.types.{ ArrayType, StringType, StructField, StructType }

object SparkValidator {

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
   * Validate a Spark DataFrame using the Bifurcation Pattern.
   *
   * Single-pass: adds _dq_errors column per row. Use report.split() to separate good/bad rows. Zero .collect() on
   * row-level data.
   */
  def validate(
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
                F.lit(result.id).alias("rule_id"),
                F.lit(rule.checkType).alias("check_type"),
                F.lit(rule.fieldName).alias("field"),
                F.lit(rule.category).alias("category"),
                F.lit(result.message.orNull).alias("message"),
                F.lit(result.expectedValue.map(_.toString).orNull).alias("expected"),
                F.lit(result.actualValue.map(_.toString).orNull).alias("actual")
              )

              // failCondition: reuse analyzer logic via column expression
              val failCond = buildFailCondition(workDf, rule)
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
  // Fail conditions per check_type — Column expressions, zero .collect()
  // -------------------------------------------------------------------------

  private def buildFailCondition(df: DataFrame, rule: RuleDefinition) = {
    import org.apache.spark.sql.Column
    import io.galileostd.sumeh.rule._

    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType

    checkType match {
      // Completeness
      case "is_complete" | "are_complete" =>
        val fields = rule.field.fold(List(_), identity)
        fields.map(f => F.col(f).isNull).reduce(_ || _)

      // Uniqueness — can't do row-level without window; mark all rows (conservative)
      case "is_unique" | "are_unique" | "is_primary_key" | "is_composite_key" =>
        val fields = rule.field.fold(List(_), identity)
        val w      = org.apache.spark.sql.expressions.Window.partitionBy(fields.map(F.col): _*)
        F.count(F.lit(1)).over(w) > 1

      // Comparison
      case "is_equal"                 => F.col(field) =!= F.lit(ruleValueToAny(rule.value))
      case "is_greater_than"          => F.col(field) <= F.lit(ruleValueToAny(rule.value))
      case "is_less_than"             => F.col(field) >= F.lit(ruleValueToAny(rule.value))
      case "is_greater_or_equal_than" => F.col(field) < F.lit(ruleValueToAny(rule.value))
      case "is_less_or_equal_than"    => F.col(field) > F.lit(ruleValueToAny(rule.value))
      case "is_positive"              => F.col(field) <= 0
      case "is_negative"              => F.col(field) >= 0
      case "is_in_millions"           => F.col(field) < 1000000L
      case "is_in_billions"           => F.col(field) < 1000000000L

      // Between
      case "is_between" =>
        val (min, max) = rule.value match {
          case Some(ListValue(lo :: hi :: Nil)) => (ruleValueToAny(Some(lo)), ruleValueToAny(Some(hi)))
          case _                                => throw new IllegalArgumentException("is_between requires [min, max]")
        }
        (F.col(field) < F.lit(min)) || (F.col(field) > F.lit(max))

      // Column comparison
      case "is_equal_than" =>
        val other = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        F.col(field) =!= F.col(other)

      // Membership
      case "is_contained_in" | "is_in" =>
        val vals = listValues(rule.value)
        !F.col(field).isin(vals: _*)
      case "not_contained_in" | "not_in" =>
        val vals = listValues(rule.value)
        F.col(field).isin(vals: _*)

      // Pattern
      case "has_pattern" =>
        val pattern = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        !F.col(field).rlike(pattern)

      case "is_legit" =>
        F.col(field).isNull || (F.trim(F.col(field)) === "")

      // Date
      case "is_today"                      => F.to_date(F.col(field)) =!= F.current_date()
      case "is_t_minus_1" | "is_yesterday" => F.to_date(F.col(field)) =!= F.date_sub(F.current_date(), 1)
      case "is_t_minus_2"                  => F.to_date(F.col(field)) =!= F.date_sub(F.current_date(), 2)
      case "is_t_minus_3"                  => F.to_date(F.col(field)) =!= F.date_sub(F.current_date(), 3)
      case "is_past_date"                  => F.to_date(F.col(field)) >= F.current_date()
      case "is_future_date"                => F.to_date(F.col(field)) <= F.current_date()
      case "is_on_weekday"                 => F.dayofweek(F.to_date(F.col(field))).isin(1, 7)
      case "is_on_weekend"                 => !F.dayofweek(F.to_date(F.col(field))).isin(1, 7)
      case "is_on_monday"                  => F.dayofweek(F.to_date(F.col(field))) =!= 2
      case "is_on_tuesday"                 => F.dayofweek(F.to_date(F.col(field))) =!= 3
      case "is_on_wednesday"               => F.dayofweek(F.to_date(F.col(field))) =!= 4
      case "is_on_thursday"                => F.dayofweek(F.to_date(F.col(field))) =!= 5
      case "is_on_friday"                  => F.dayofweek(F.to_date(F.col(field))) =!= 6
      case "is_on_saturday"                => F.dayofweek(F.to_date(F.col(field))) =!= 7
      case "is_on_sunday"                  => F.dayofweek(F.to_date(F.col(field))) =!= 1

      case "is_date_between" =>
        val (start, end) = rule.value match {
          case Some(ListValue(StringValue(s) :: StringValue(e) :: Nil)) => (s, e)
          case _ => throw new IllegalArgumentException("is_date_between requires [start, end]")
        }
        val dc = F.to_date(F.col(field))
        (dc < F.to_date(F.lit(start))) || (dc > F.to_date(F.lit(end)))

      case "is_date_after" =>
        val target = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        F.to_date(F.col(field)) <= F.to_date(F.lit(target))

      case "is_date_before" =>
        val target = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        F.to_date(F.col(field)) >= F.to_date(F.lit(target))

      case other =>
        throw new IllegalArgumentException(s"No fail condition defined for: $other")
    }
  }

  private def ruleValueToAny(v: Option[io.galileostd.sumeh.rule.RuleValue]): Any = v match {
    case Some(io.galileostd.sumeh.rule.StringValue(s)) => s
    case Some(io.galileostd.sumeh.rule.LongValue(l))   => l
    case Some(io.galileostd.sumeh.rule.DoubleValue(d)) => d
    case _                                                => null
  }

  private def listValues(v: Option[io.galileostd.sumeh.rule.RuleValue]): Seq[Any] = v match {
    case Some(io.galileostd.sumeh.rule.ListValue(items)) => items.map(v => ruleValueToAny(Some(v)))
    case _                                                  => Seq.empty
  }

  private def skippedResult(rule: RuleDefinition, level: ValidationLevel, reason: String) =
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = level,
      category = rule.category,
      status = ValidationStatus.ERROR,
      message = Some(s"Skipped: $reason")
    )

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
