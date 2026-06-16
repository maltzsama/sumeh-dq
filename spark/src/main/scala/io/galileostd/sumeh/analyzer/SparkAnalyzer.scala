package io.galileostd.sumeh.spark.analyzer

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.{ DoubleValue, ListValue, LongValue, RuleDefinition, RuleValue, StringValue }
import org.apache.spark.sql.{ functions => F, DataFrame }

// ============================================================================
// Base trait
// ============================================================================

trait SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult

  protected def fieldName(rule: RuleDefinition): String =
    rule.field.fold(identity, _.mkString(","))

  protected def requireField(df: DataFrame, field: String): Unit =
    if (!df.columns.contains(field))
      throw new IllegalArgumentException(s"Field '$field' not found in DataFrame")

  protected def passRate(total: Long, failCount: Long): Double =
    if (total > 0) (total - failCount).toDouble / total else 1.0
}

// ============================================================================
// Completeness
// ============================================================================

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

object ComparisonAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    val threshold = ruleValueToAny(rule.value)
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

  private def ruleValueToAny(v: Option[RuleValue]): Any = v match {
    case Some(StringValue(s)) => s
    case Some(LongValue(l))   => l
    case Some(DoubleValue(d)) => d
    case _                    => null
  }
}

object BetweenAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val (minVal, maxVal) = rule.value match {
      case Some(ListValue(lo :: hi :: Nil)) =>
        (ruleValueToAny(Some(lo)), ruleValueToAny(Some(hi)))
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

  private def ruleValueToAny(v: Option[RuleValue]): Any = v match {
    case Some(StringValue(s)) => s
    case Some(LongValue(l))   => l
    case Some(DoubleValue(d)) => d
    case _                    => null
  }
}

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

object DateAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    requireField(df, field)

    val dateCol = F.to_date(F.col(field))
    val today   = F.current_date()

    val failCond = checkType match {
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

object DateBetweenAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field = rule.field.fold(identity, _.head)
    requireField(df, field)

    val (start, end) = rule.value match {
      case Some(ListValue(StringValue(s) :: StringValue(e) :: Nil)) => (s, e)
      case _ => throw new IllegalArgumentException("is_date_between requires value=[start, end]")
    }

    val dateCol  = F.to_date(F.col(field))
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

object DateComparisonAnalyzer extends SparkAnalyzer {
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val field     = rule.field.fold(identity, _.head)
    val checkType = rule.checkType
    val target = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException(s"$checkType requires a date value"))
    requireField(df, field)

    val dateCol    = F.to_date(F.col(field))
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
}
