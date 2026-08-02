package io.galileostd.sumeh.spark.analyzer

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.{ DoubleValue, ListValue, LongValue, RuleDefinition, RuleValue, StringValue }
import io.galileostd.sumeh.spark.expr.FailCondition
import org.apache.spark.sql.{ functions => F, DataFrame }

// ============================================================================
// Base trait
// ============================================================================

/**
 * Computes a `MetricResult` for a rule on a Spark DataFrame.
 *
 * Analyzers are the "pure computation" half of validation: they aggregate the DataFrame into a metric and have no
 * opinion about thresholds or pass/fail semantics (that is the Constraint's job). The same input always yields the same
 * output, which keeps validation deterministic and testable.
 */
trait SparkAnalyzer {

  /**
   * Computes the metric for the given rule.
   *
   * @param df The DataFrame to analyze.
   * @param rule The rule whose check determines what to measure.
   * @return The computed metric.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult

  /**
   * Flattened column name(s) of the rule — a single name or a comma-joined list.
   *
   * @return The rule's `fieldName`.
   */
  protected def fieldName(rule: RuleDefinition): String =
    rule.field.fold(identity, _.mkString(","))

  /**
   * Throws if the field is not a column of the DataFrame.
   *
   * @param df The DataFrame.
   * @param field The column name to check.
   * @throws java.lang.IllegalArgumentException when the field is absent.
   */
  protected def requireField(df: DataFrame, field: String): Unit =
    if (!df.columns.exists(_.equalsIgnoreCase(field)))
      throw new IllegalArgumentException(s"Field '$field' not found in DataFrame")

  /**
   * Pass rate of a metric: `(total - failCount) / total`.
   *
   * Returns `1.0` when there are no rows, so an empty dataset passes a rule rather than erroring.
   *
   * @param total Total row count.
   * @param failCount Number of failing rows.
   * @return The pass rate in `[0.0, 1.0]`.
   */
  protected def passRate(total: Long, failCount: Long): Double =
    if (total > 0) (total - failCount).toDouble / total else 1.0
}

// ============================================================================
// Completeness
// ============================================================================

/**
 * Analyzer for `is_complete` — the pass rate of a single field having no nulls.
 *
 * Counts rows where the field is null and reports the pass rate `1 - null_count/total`.
 */
object CompletenessAnalyzer extends SparkAnalyzer {

  /**
   * Computes the completeness metric for the rule's field.
   *
   * @param df The DataFrame.
   * @param rule A rule with a single-column field.
   * @return a completeness MetricResult; value = non-null fraction, metadata = null/total counts
   * @throws java.lang.IllegalArgumentException when the field is not in the DataFrame.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("null_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val nullCount = result.getAs[Long]("null_count")

    MetricResult(
      metricType = "completeness",
      field = rule.field,
      value = passRate(total, nullCount),
      totalRows = total,
      metadata = Map("null_count" -> nullCount, "total_count" -> total)
    )
  }
}

/**
 * Analyzer for `are_complete` — the pass rate of a set of fields being simultaneously non-null.
 *
 * A row fails when any of the given fields is null.
 */
object MultiFieldCompletenessAnalyzer extends SparkAnalyzer {

  /**
   * Computes the multi-field completeness metric.
   *
   * @param df The DataFrame.
   * @param rule A rule whose field is `Right(List(...))` of columns.
   * @return a MetricResult; value = non-null fraction across all fields, metadata = field list
   * @throws java.lang.IllegalArgumentException when any field is not in the DataFrame.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val fields = rule.field.fold(List(_), identity)
    fields.foreach(requireField(df, _))

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("incomplete_count")
      )
      .collect()(0)

    val total           = result.getAs[Long]("total")
    val incompleteCount = result.getAs[Long]("incomplete_count")

    MetricResult(
      metricType = "multi_field_completeness",
      field = rule.field,
      value = passRate(total, incompleteCount),
      totalRows = total,
      metadata = Map("incomplete_count" -> incompleteCount, "total_count" -> total, "fields" -> fields)
    )
  }
}

// ============================================================================
// Uniqueness
// ============================================================================

/**
 * Analyzer for `is_unique` — the pass rate of a single field having no duplicate values.
 *
 * Counts the total rows that participate in a duplicate group and reports `1 - dup/total`. Requires a full scan, so it
 * is batch-only (the Spark Registry gates it).
 */
object UniquenessAnalyzer extends SparkAnalyzer {

  /**
   * Computes the uniqueness metric for the rule's field.
   *
   * @param df The DataFrame.
   * @param rule A rule with a single-column field.
   * @return a uniqueness MetricResult; metadata holds duplicate and total counts
   * @throws java.lang.IllegalArgumentException when the field is not in the DataFrame.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val total = df.count()
    val dupCount = df
      .groupBy(field)
      .count()
      .filter(F.col("count") > 1)
      .agg(F.sum("count").alias("dup_count"))
      .collect()(0)
      .getAs[Long]("dup_count")

    MetricResult(
      metricType = "uniqueness",
      field = rule.field,
      value = passRate(total, dupCount),
      totalRows = total,
      metadata = Map("duplicate_count" -> dupCount, "total_count" -> total)
    )
  }
}

/**
 * Analyzer for `are_unique` — the pass rate of a combination of fields having no duplicate combinations.
 *
 * Counts rows in duplicate groups by grouping on all the rule's columns together.
 */
object MultiFieldUniquenessAnalyzer extends SparkAnalyzer {

  /**
   * Computes the multi-field uniqueness metric.
   *
   * @param df The DataFrame.
   * @param rule A rule whose field is `Right(List(...))` of columns.
   * @return A metric whose `value` is the non-duplicate fraction; `metadata` lists `fields`.
   * @throws java.lang.IllegalArgumentException when any field is not in the DataFrame.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val fields = rule.field.fold(List(_), identity)
    fields.foreach(requireField(df, _))

    val total = df.count()
    val dupCount = df
      .groupBy(fields.map(F.col): _*)
      .count()
      .filter(F.col("count") > 1)
      .agg(F.sum("count").alias("dup_count"))
      .collect()(0)
      .getAs[Long]("dup_count")

    MetricResult(
      metricType = "multi_field_uniqueness",
      field = rule.field,
      value = passRate(total, dupCount),
      totalRows = total,
      metadata = Map("duplicate_count" -> dupCount, "total_count" -> total, "fields" -> fields)
    )
  }
}

// ============================================================================
// Comparison
// ============================================================================

/**
 * Analyzer for the comparison rule family: `is_equal`, `is_equal_than`, `is_greater_than`, `is_less_than`,
 * `is_greater_or_equal_than`, `is_less_or_equal_than`, `is_positive`, `is_negative`, `is_in_millions`,
 * `is_in_billions`.
 *
 * Each `check_type` maps to a column predicate; a row fails when the predicate is false (the fail condition is the
 * negation of the intended comparison).
 */
object ComparisonAnalyzer extends SparkAnalyzer {

  /**
   * Computes the comparison metric, dispatching on `rule.checkType`.
   *
   * @param df The DataFrame.
   * @param rule A rule with a numeric field and (for most checks) a `value` threshold.
   * @return a MetricResult; value = pass fraction, metadata = fail_count/total_count/threshold
   * @throws java.lang.IllegalArgumentException if the field is missing or `check_type` is not a known comparison
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    val threshold = rule.value.map(RuleValue.toAny).orNull
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "comparison",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "total_count" -> total, "threshold" -> threshold)
    )
  }
}

/**
 * Analyzer for `is_between` — the pass rate of rows whose value lies inside a `[min, max]` range.
 *
 * A row fails when it is strictly below `min` or strictly above `max`.
 */
object BetweenAnalyzer extends SparkAnalyzer {

  /**
   * Computes the between metric from the rule's `value` list `[min, max]`.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a `ListValue` of exactly two elements.
   * @return A metric whose `value` is the in-range fraction; `metadata` holds `min`/`max`.
   * @throws java.lang.IllegalArgumentException when the field is missing or `value` is not a two-element list.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val (lo, hi) = FailCondition.requirePair(rule, "is_between requires value=[min, max]")
    val minVal   = RuleValue.toAny(lo)
    val maxVal   = RuleValue.toAny(hi)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "between",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "min" -> minVal, "max" -> maxVal)
    )
  }
}

/**
 * Analyzer for `is_equal_than` — the pass rate of rows where a field equals another column.
 *
 * A row fails when the two column values differ.
 */
object ColumnComparisonAnalyzer extends SparkAnalyzer {

  /**
   * Computes the column-comparison metric.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` names the other column to compare against.
   * @return A metric whose `value` is the equality fraction; `metadata` holds `compared_to`.
   * @throws java.lang.IllegalArgumentException when either column is missing or `value` has no column name.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field      = rule.field.fold(identity, _.head)
    val otherField = FailCondition.requireString(rule, "is_equal_than requires a column name as value")
    requireField(df, field)
    requireField(df, otherField)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "column_comparison",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "compared_to" -> otherField)
    )
  }
}

// ============================================================================
// Membership
// ============================================================================

/**
 * Analyzer for the membership rule family: `is_contained_in` / `is_in` and `not_contained_in` / `not_in`.
 *
 * `is_*` fails a row when its value is NOT in the allowed list; `not_*` fails a row when its value IS in the disallowed
 * list.
 */
object MembershipAnalyzer extends SparkAnalyzer {

  /**
   * Computes the membership metric from the rule's list-valued `value`.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a `ListValue` of allowed/disallowed items.
   * @return A metric whose `value` is the pass fraction; `metadata` holds `fail_count` and `values`.
   * @throws java.lang.IllegalArgumentException when the field is missing or `value` is not a list.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    requireField(df, field)

    val items = FailCondition.requireList(rule, "Membership requires a list of values")
    val values: Seq[Any] = items.map {
      case StringValue(s) => s
      case LongValue(l)   => l
      case DoubleValue(d) => d
      case other          => other.toString
    }

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "membership",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "values" -> values)
    )
  }
}

// ============================================================================
// Pattern
// ============================================================================

/**
 * Analyzer for `has_pattern` — the pass rate of rows whose value matches a regular expression.
 *
 * A row fails when the field does not `rlike` the pattern.
 */
object PatternAnalyzer extends SparkAnalyzer {

  /**
   * Computes the regex-match metric from the rule's pattern `value`.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a `StringValue` regex.
   * @return A metric whose `value` is the matching fraction; `metadata` holds `pattern`.
   * @throws java.lang.IllegalArgumentException when the field is missing or the pattern is not a string.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field   = rule.field.fold(identity, _.head)
    val pattern = FailCondition.requireString(rule, "has_pattern requires a regex pattern")
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "pattern",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "pattern" -> pattern)
    )
  }
}

/**
 * Analyzer for `is_legit` — the pass rate of rows that are non-null and non-blank.
 *
 * A row fails when the field is null or trims to an empty string.
 */
object LegitAnalyzer extends SparkAnalyzer {

  /**
   * Computes the "legit" metric for the rule's field.
   *
   * @param df The DataFrame.
   * @param rule A rule with a single-column field.
   * @return A metric whose `value` is the fraction of non-blank, non-null rows.
   * @throws java.lang.IllegalArgumentException when the field is not in the DataFrame.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "legit",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount)
    )
  }
}

// ============================================================================
// Date
// ============================================================================

/**
 * Analyzer for the date rule family: `is_today`, `is_t_minus_1/2/3`, `is_yesterday`, `is_past_date`, `is_future_date`,
 * `is_on_weekday`, `is_on_weekend`, `is_on_monday`...`is_on_sunday`, and `all_date_checks`.
 *
 * All comparisons operate on a parsed date column via `DateExpr.safeToDate` (ANSI-safe, null instead of throwing). Note
 * `is_on_weekday` fails on Saturday (1) and Sunday (7) because Spark's `dayofweek` starts on Sunday; `is_on_weekend` is
 * its complement.
 */
object DateAnalyzer extends SparkAnalyzer {

  /**
   * Computes the date metric, dispatching on `rule.checkType`.
   *
   * @param df The DataFrame.
   * @param rule A rule with a date field and one of the supported date `check_type`s.
   * @return a MetricResult; value = date-predicate pass fraction, metadata = fail_count/check_type
   * @throws java.lang.IllegalArgumentException if the field is missing or `check_type` is not a known date check
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "date",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "check_type" -> checkType)
    )
  }
}

/**
 * Analyzer for `is_date_between` — the pass rate of rows whose date lies inside a `[start, end]` range.
 *
 * A row fails when its parsed date is strictly before `start` or strictly after `end`.
 */
object DateBetweenAnalyzer extends SparkAnalyzer {

  /**
   * Computes the date-range metric from the rule's `value` list `[start, end]`.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a `ListValue` of two date strings.
   * @return A metric whose `value` is the in-range fraction; `metadata` holds `start`/`end`.
   * @throws java.lang.IllegalArgumentException when the field is missing or `value` is not two date strings.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val (start, end) = FailCondition.requirePair(rule, "is_date_between requires value=[start, end]") match {
      case (StringValue(s), StringValue(e)) => (s, e)
      case _ => throw new IllegalArgumentException("is_date_between requires [start, end] date strings")
    }

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "date_between",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "start" -> start, "end" -> end)
    )
  }
}

/**
 * Analyzer for `is_date_after` / `is_date_before` — the pass rate of rows whose date is after/before a target date.
 *
 * `is_date_after` fails a row when its date is on or before the target; `is_date_before` fails when on or after it.
 */
object DateComparisonAnalyzer extends SparkAnalyzer {

  /**
   * Computes the date-comparison metric against the rule's target date `value`.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a date string; `check_type` is `is_date_after` or `is_date_before`.
   * @return A metric whose `value` is the fraction satisfying the comparison; `metadata` holds `target`.
   * @throws java.lang.IllegalArgumentException if the field is missing, `value` has no date, or `check_type` is unknown
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    val target    = FailCondition.requireString(rule, s"$checkType requires a date value")
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "date_comparison",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "target" -> target)
    )
  }
}

// ============================================================================
// Aggregation (TABLE level)
// ============================================================================

/**
 * Analyzer for TABLE-level aggregations: `has_min`, `has_max`, `has_sum`, `has_mean`, `has_std`, `has_cardinality`,
 * `has_entropy`, `has_infogain`.
 *
 * These reduce the whole column to a single number (the metric `value`), which the AggregationConstraint later compares
 * against the rule's expected `value` within a relative threshold. Batch-only.
 */
object AggregationAnalyzer extends SparkAnalyzer {

  /**
   * Computes the requested aggregation of the rule's field.
   *
   * @param df The DataFrame.
   * @param rule A rule with a numeric field and one of the supported aggregation `check_type`s.
   * @return A metric whose `value` is the aggregated number; `metadata` holds `metric` and `value`.
   * @throws java.lang.IllegalArgumentException when the field is missing or the `check_type` is unknown.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    requireField(df, field)

    val actual = checkType match {
      case "has_min"         => df.agg(F.min(field)).collect()(0).getAs[Any](0)
      case "has_max"         => df.agg(F.max(field)).collect()(0).getAs[Any](0)
      case "has_sum"         => df.agg(F.sum(field)).collect()(0).getAs[Any](0)
      case "has_mean"        => df.agg(F.mean(field)).collect()(0).getAs[Any](0)
      case "has_std"         => df.agg(F.stddev(field)).collect()(0).getAs[Any](0)
      case "has_cardinality" => df.agg(F.countDistinct(field)).collect()(0).getAs[Any](0)
      case "has_entropy"     => entropy(df, field)
      case "has_infogain"    => normalizedEntropy(df, field)
      case other             => throw new IllegalArgumentException(s"Unknown aggregation: $other")
    }

    val value = actual match {
      case n: Number => n.doubleValue()
      case null =>
        throw new IllegalArgumentException(
          s"${rule.checkType} on '${fieldName(rule)}': aggregation returned null (empty or all-null column)"
        )
      case other =>
        throw new IllegalArgumentException(
          s"${rule.checkType} not applicable to column '${fieldName(rule)}': " +
            s"non-numeric result (${other.getClass.getSimpleName})"
        )
    }

    MetricResult(
      metricType = "aggregation",
      field = rule.field,
      value = value,
      totalRows = -1L, // profiler computes row count itself; no extra count() job here
      metadata = Map("metric" -> checkType, "value" -> value)
    )
  }

  /**
   * Shannon entropy `-Σ pᵢ·log₂(pᵢ)` of the field's value distribution.
   *
   * Computed over non-null values only. Returns `0.0` for an empty or all-null column.
   *
   * @param df The DataFrame.
   * @param field The column to measure.
   * @return The entropy in bits, in `[0, log₂(cardinality)]`.
   */
  private def entropy(df: DataFrame, field: String): Double = {
    val counts = df.select(field).na.drop().groupBy(field).count()
    val total  = counts.agg(F.sum("count")).collect()(0).getAs[Long](0)
    if (total == 0L) 0.0
    else
      counts
        .withColumn("p", F.col("count") / F.lit(total.toDouble))
        .agg(F.sum(F.negate(F.col("p")) * F.log2(F.col("p"))))
        .collect()(0)
        .getAs[Double](0)
  }

  /**
   * Normalized entropy `H / log₂(cardinality)`.
   *
   * `1.0` means a uniform distribution, `0.0` a single value. Returns `0.0` when the column has fewer than two distinct
   * non-null values.
   *
   * @param df The DataFrame.
   * @param field The column to measure.
   * @return The normalized entropy in `[0.0, 1.0]`.
   */
  private def normalizedEntropy(df: DataFrame, field: String): Double = {
    val h = entropy(df, field)
    val distinct = df
      .select(field)
      .na
      .drop()
      .agg(F.countDistinct(field))
      .collect()(0)
      .getAs[Long](0)
    if (distinct <= 1L) 0.0 else h / (math.log(distinct.toDouble) / math.log(2.0))
  }
}

/**
 * Analyzer for `validate_date_format` — the pass rate of rows whose string parses with the expected format.
 *
 * A row fails when it is non-null but `try_to_timestamp` cannot parse it with the rule's format string.
 */
object DateFormatAnalyzer extends SparkAnalyzer {

  /**
   * Computes the date-format metric from the rule's format `value`.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a `StringValue` pattern (e.g. `"yyyy-MM-dd"`).
   * @return A metric whose `value` is the fraction of parseable rows; `metadata` holds `format`.
   * @throws java.lang.IllegalArgumentException when the field is missing or the format is not a string.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field  = rule.field.fold(identity, _.head)
    val format = FailCondition.requireString(rule, "validate_date_format requires a format string as value")
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "date_format",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "format" -> format)
    )
  }
}

/**
 * Analyzer for `validate_schema` — whether the DataFrame schema matches a serialized schema contract.
 *
 * The rule's `value` is a JSON string holding a schema contract (see `SchemaDef.fromMap`). The metric `value` is `1.0`
 * when the schema passes, `0.0` otherwise, with the failure breakdown in `metadata`.
 */
object SchemaAnalyzer extends SparkAnalyzer {

  /**
   * Computes the schema metric by validating the DataFrame against the serialized contract.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a JSON schema string.
   * @return a MetricResult; `1.0`=pass, `0.0`=fail; metadata details missing columns, type errors, and extras
   * @throws java.lang.IllegalArgumentException when `value` is missing or is not a valid JSON schema string.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    // value contém o SchemaDef serializado como JSON string
    val schemaJson = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException("validate_schema requires a JSON schema as value"))

    import io.galileostd.sumeh.schema.{ SchemaDef, ColumnDef }
    import io.galileostd.sumeh.spark.schema.SparkSchemaValidator
    import upickle.default._

    val schemaMap = read[Map[String, ujson.Value]](schemaJson).map { case (k, v) => k -> ujsonToAny(v) }
    val schemaDef = SchemaDef.fromMap(schemaMap)
    val report    = SparkSchemaValidator.validate(df, schemaDef)

    MetricResult(
      metricType = "schema",
      field = Left("*"),
      value = if (report.passed) 1.0 else 0.0,
      totalRows = df.count(),
      metadata = Map(
        "passed"          -> report.passed,
        "missing_cols"    -> report.missingCols,
        "type_errors"     -> report.typeErrors,
        "metadata_errors" -> report.metadataErrors,
        "extra_cols"      -> report.extraCols
      )
    )
  }

  /**
   * Converts a ujson value into plain JVM types for [[io.galileostd.sumeh.schema.SchemaDef.fromMap]].
   *
   * Whole numbers become `Long`, everything else keeps its natural type; arrays and objects are converted recursively.
   *
   * @param v The JSON value.
   * @return The plain JVM value.
   */
  private def ujsonToAny(v: ujson.Value): Any = v match {
    case ujson.Str(s)  => s
    case ujson.Num(n)  => if (n == n.toLong) n.toLong else n
    case ujson.Bool(b) => b
    case ujson.Null    => null
    case ujson.Arr(a)  => a.toList.map(ujsonToAny)
    case ujson.Obj(o)  => o.map { case (k, vv) => k -> ujsonToAny(vv) }.toMap
  }
}

/**
 * Analyzer for `satisfies` — the pass rate of rows that match a custom SQL condition.
 *
 * A row fails when the SQL expression evaluates to false. The condition is evaluated with `F.expr`, so it can reference
 * any column of the DataFrame.
 */
object SatisfiesAnalyzer extends SparkAnalyzer {

  /**
   * Computes the SQL-condition metric from the rule's condition `value`.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a `StringValue` SQL expression.
   * @return A metric whose `value` is the fraction of matching rows; `metadata` holds `condition`.
   * @throws java.lang.IllegalArgumentException when `value` is not a SQL condition string.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val condition = FailCondition.requireString(rule, "satisfies requires a SQL condition as value")

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(FailCondition(rule), 1).otherwise(0)).alias("fail_count")
      )
      .collect()(0)

    val total     = result.getAs[Long]("total")
    val failCount = result.getAs[Long]("fail_count")

    MetricResult(
      metricType = "satisfies",
      field = rule.field,
      value = passRate(total, failCount),
      totalRows = total,
      metadata = Map("fail_count" -> failCount, "condition" -> condition)
    )
  }
}
