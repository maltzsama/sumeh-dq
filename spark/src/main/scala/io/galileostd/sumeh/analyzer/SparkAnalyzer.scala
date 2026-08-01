package io.galileostd.sumeh.spark.analyzer

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.{ DoubleValue, ListValue, LongValue, RuleDefinition, RuleValue, StringValue }
import io.galileostd.sumeh.spark.DateExpr
import org.apache.spark.sql.{ functions => F, DataFrame }

// ============================================================================
// Base trait
// ============================================================================

/**
 * Computes a MetricResult for a rule on a Spark DataFrame — pure computation, no rule opinion.
 *
 * Analyzers are pure: the same input always yields the same output, and they never apply thresholds (that is the
 * Constraint's job).
 */
trait SparkAnalyzer {

  /**
   * Computes the metric for the given rule.
   *
   * Args: df: The DataFrame. rule: The rule to analyze.
   *
   * Returns: The computed metric.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult

  /** Flattened column name(s) of the rule. */
  protected def fieldName(rule: RuleDefinition): String =
    rule.field.fold(identity, _.mkString(","))

  /** Throws if the field is not present in the DataFrame. */
  protected def requireField(df: DataFrame, field: String): Unit =
    if (!df.columns.contains(field))
      throw new IllegalArgumentException(s"Field '$field' not found in DataFrame")

  /** Pass rate = (total - failCount) / total; 1.0 when there are no rows. */
  protected def passRate(total: Long, failCount: Long): Double =
    if (total > 0) (total - failCount).toDouble / total else 1.0
}

// ============================================================================
// Completeness
// ============================================================================

/** Analyzer for is_complete — measures null counts on a single field. */
object CompletenessAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col(field).isNull, 1).otherwise(0)).alias("null_count")
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

/** Analyzer for are_complete — measures rows where any of several fields is null. */
object MultiFieldCompletenessAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val fields = rule.field.fold(List(_), identity)
    fields.foreach(requireField(df, _))

    val anyNull = fields.map(f => F.col(f).isNull).reduce(_ || _)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(anyNull, 1).otherwise(0)).alias("incomplete_count")
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

/** Analyzer for is_unique — measures duplicate values of a single field. */
object UniquenessAnalyzer extends SparkAnalyzer {
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

/** Analyzer for are_unique — measures duplicate combinations of several fields. */
object MultiFieldUniquenessAnalyzer extends SparkAnalyzer {
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

/** Analyzer for comparison rules (is_equal, is_greater_than, is_positive, ...). */
object ComparisonAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    val threshold = rule.value.map(RuleValue.toAny).orNull
    requireField(df, field)

    val failCond = checkType match {
      case "is_equal"                 => F.col(field) =!= F.lit(threshold)
      case "is_greater_than"          => F.col(field) <= F.lit(threshold)
      case "is_less_than"             => F.col(field) >= F.lit(threshold)
      case "is_greater_or_equal_than" => F.col(field) < F.lit(threshold)
      case "is_less_or_equal_than"    => F.col(field) > F.lit(threshold)
      case "is_positive"              => F.col(field) <= 0
      case "is_negative"              => F.col(field) >= 0
      case "is_in_millions"           => F.col(field) < 1000000L
      case "is_in_billions"           => F.col(field) < 1000000000L
      case other                      => throw new IllegalArgumentException(s"Unknown comparison: $other")
    }

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for is_between — measures rows outside a [min, max] range. */
object BetweenAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val (minVal, maxVal) = rule.value match {
      case Some(ListValue(lo :: hi :: Nil)) =>
        (RuleValue.toAny(lo), RuleValue.toAny(hi))
      case _ =>
        throw new IllegalArgumentException("is_between requires value=[min, max]")
    }

    val failCond = (F.col(field) < F.lit(minVal)) || (F.col(field) > F.lit(maxVal))

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for is_equal_than — compares a field against another column. */
object ColumnComparisonAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    val otherField = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException("is_equal_than requires a column name as value"))
    requireField(df, field)
    requireField(df, otherField)

    val failCond = F.col(field) =!= F.col(otherField)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for membership rules (is_contained_in, not_contained_in, ...). */
object MembershipAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    requireField(df, field)

    val values: Seq[Any] = rule.value match {
      case Some(ListValue(items)) =>
        items.map {
          case StringValue(s) => s
          case LongValue(l)   => l
          case DoubleValue(d) => d
          case other          => other.toString
        }
      case _ => throw new IllegalArgumentException("Membership requires a list of values")
    }

    val failCond = checkType match {
      case "is_contained_in" | "is_in" => !F.col(field).isin(values: _*)
      case _                           => F.col(field).isin(values: _*)
    }

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for has_pattern — measures rows that don't match a regex. */
object PatternAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    val pattern = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException("has_pattern requires a regex pattern"))
    requireField(df, field)

    val failCond = !F.col(field).rlike(pattern)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for is_legit — measures null or whitespace-only values. */
object LegitAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val failCond = F.col(field).isNull || (F.trim(F.col(field)) === "")

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for date rules (is_today, is_past_date, is_on_weekday, all_date_checks, ...). */
object DateAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    requireField(df, field)

    val dateCol = DateExpr.safeToDate(F.col(field))
    val today   = F.current_date()

    val failCond = checkType match {
      case "all_date_checks"               => F.col(field).isNotNull && DateExpr.safeToDate(F.col(field)).isNull
      case "is_today"                      => dateCol =!= today
      case "is_t_minus_1" | "is_yesterday" => dateCol =!= F.date_sub(today, 1)
      case "is_t_minus_2"                  => dateCol =!= F.date_sub(today, 2)
      case "is_t_minus_3"                  => dateCol =!= F.date_sub(today, 3)
      case "is_past_date"                  => dateCol >= today
      case "is_future_date"                => dateCol <= today
      case "is_on_weekday"                 => F.dayofweek(dateCol).isin(1, 7)
      case "is_on_weekend"                 => !F.dayofweek(dateCol).isin(1, 7)
      case "is_on_monday"                  => F.dayofweek(dateCol) =!= 2
      case "is_on_tuesday"                 => F.dayofweek(dateCol) =!= 3
      case "is_on_wednesday"               => F.dayofweek(dateCol) =!= 4
      case "is_on_thursday"                => F.dayofweek(dateCol) =!= 5
      case "is_on_friday"                  => F.dayofweek(dateCol) =!= 6
      case "is_on_saturday"                => F.dayofweek(dateCol) =!= 7
      case "is_on_sunday"                  => F.dayofweek(dateCol) =!= 1
      case other                           => throw new IllegalArgumentException(s"Unknown date check: $other")
    }

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for is_date_between — measures rows outside a [start, end] date range. */
object DateBetweenAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val (start, end) = rule.value match {
      case Some(ListValue(StringValue(s) :: StringValue(e) :: Nil)) => (s, e)
      case _ => throw new IllegalArgumentException("is_date_between requires value=[start, end]")
    }

    val dateCol  = DateExpr.safeToDate(F.col(field))
    val failCond = (dateCol < F.to_date(F.lit(start))) || (dateCol > F.to_date(F.lit(end)))

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for is_date_after / is_date_before — compares a date field against a target date. */
object DateComparisonAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    val target = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException(s"$checkType requires a date value"))
    requireField(df, field)

    val dateCol    = DateExpr.safeToDate(F.col(field))
    val targetDate = F.to_date(F.lit(target))

    val failCond = checkType match {
      case "is_date_after"  => dateCol <= targetDate
      case "is_date_before" => dateCol >= targetDate
      case other            => throw new IllegalArgumentException(s"Unknown date comparison: $other")
    }

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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
 * Analyzer for TABLE-level aggregations (has_min, has_max, has_sum, has_mean, has_std, has_cardinality, has_entropy,
 * has_infogain).
 */
object AggregationAnalyzer extends SparkAnalyzer {
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
      case _         => 0.0
    }

    MetricResult(
      metricType = "aggregation",
      field = rule.field,
      value = value,
      totalRows = df.count(),
      metadata = Map("metric" -> checkType, "value" -> value)
    )
  }

  /** Shannon entropy -Σ pᵢ·log₂(pᵢ) of the field's value distribution. */
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

  /** Normalized entropy H / log2(cardinality); 1.0 = uniform, 0.0 = single value. */
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

/** Analyzer for validate_date_format — measures rows that don't parse with the expected format. */
object DateFormatAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    val format = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException("validate_date_format requires a format string as value"))
    requireField(df, field)

    val failCond = F.try_to_timestamp(F.col(field), F.lit(format)).isNull && F.col(field).isNotNull

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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

/** Analyzer for validate_schema — validates the DataFrame schema against a serialized SchemaDef. */
object SchemaAnalyzer extends SparkAnalyzer {
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

  /** Converts a ujson value into plain JVM types for SchemaDef.fromMap. */
  private def ujsonToAny(v: ujson.Value): Any = v match {
    case ujson.Str(s)  => s
    case ujson.Num(n)  => if (n == n.toLong) n.toLong else n
    case ujson.Bool(b) => b
    case ujson.Null    => null
    case ujson.Arr(a)  => a.toList.map(ujsonToAny)
    case ujson.Obj(o)  => o.map { case (k, vv) => k -> ujsonToAny(vv) }.toMap
  }
}

/** Analyzer for satisfies — measures rows that don't match a custom SQL condition. */
object SatisfiesAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val condition = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException("satisfies requires a SQL condition as value"))

    val failCond = !F.expr(condition)

    val result = df
      .agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(failCond, 1).otherwise(0)).alias("fail_count")
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
