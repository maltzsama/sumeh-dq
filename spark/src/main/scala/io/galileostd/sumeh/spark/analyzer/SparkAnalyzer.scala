package io.galileostd.sumeh.spark.analyzer

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.{ RuleDefinition, StringValue }
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
 *
 * Internal engine component — not part of the public API. The only public entry point is
 * [[io.galileostd.sumeh.spark.SparkValidator.validate]], which is single-pass by construction. Only the rules that
 * cannot share the single aggregation (uniqueness, TABLE-level) keep an analyzer; every ROW rule's metric is computed
 * by the shared `agg` in `SparkValidator`.
 */
private[sumeh] trait SparkAnalyzer {

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
// Uniqueness
// ============================================================================

/**
 * Analyzer for `is_unique` — the pass rate of a single field having no duplicate values.
 *
 * Counts the total rows that participate in a duplicate group and reports `1 - dup/total`. Computed in a single Spark
 * job: the sum of every group's size is the total row count, so both numbers come from the same `groupBy` — no separate
 * `df.count()` scan. Batch-only (the Spark Registry gates it).
 */
private[sumeh] object UniquenessAnalyzer extends SparkAnalyzer {

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

    val row = df
      .groupBy(field)
      .count()
      .agg(
        F.sum("count").alias("total"),
        F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0L)).alias("dup_count")
      )
      .collect()(0)

    val total    = if (row.isNullAt(0)) 0L else row.getLong(0)
    val dupCount = if (row.isNullAt(1)) 0L else row.getLong(1)

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
 * Counts rows in duplicate groups by grouping on all the rule's columns together, in a single Spark job (the sum of
 * every group's size is the total row count — no separate `df.count()`).
 */
private[sumeh] object MultiFieldUniquenessAnalyzer extends SparkAnalyzer {

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

    val row = df
      .groupBy(fields.map(F.col): _*)
      .count()
      .agg(
        F.sum("count").alias("total"),
        F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0L)).alias("dup_count")
      )
      .collect()(0)

    val total    = if (row.isNullAt(0)) 0L else row.getLong(0)
    val dupCount = if (row.isNullAt(1)) 0L else row.getLong(1)

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
// Aggregation (TABLE level)
// ============================================================================

/**
 * Analyzer for TABLE-level aggregations: `has_min`, `has_max`, `has_sum`, `has_mean`, `has_std`, `has_cardinality`,
 * `has_entropy`, `has_infogain`.
 *
 * These reduce the whole column to a single number (the metric `value`), which the AggregationConstraint later compares
 * against the rule's expected `value` within a relative threshold. Batch-only.
 */
private[sumeh] object AggregationAnalyzer extends SparkAnalyzer {

  /**
   * Computes the requested aggregation of the rule's field.
   *
   * @param df The DataFrame.
   * @param rule A rule with one of the supported aggregation `check_type`s. Numeric aggregations (`has_min`, `has_max`,
   *             `has_sum`, `has_mean`, `has_std`) require a numeric field; `has_cardinality`, `has_entropy`, and
   *             `has_infogain` work on any field type.
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
 * Analyzer for `validate_schema` — whether the DataFrame schema matches a serialized schema contract.
 *
 * The rule's `value` is a JSON string holding a schema contract (see `SchemaDef.fromMap`). The metric `value` is `1.0`
 * when the schema passes, `0.0` otherwise, with the failure breakdown in `metadata`.
 */
private[sumeh] object SchemaAnalyzer extends SparkAnalyzer {

  /**
   * Computes the schema metric by validating the DataFrame against the serialized contract.
   *
   * @param df The DataFrame.
   * @param rule A rule whose `value` is a JSON schema string.
   * @return a MetricResult; `1.0`=pass, `0.0`=fail; metadata details missing columns, type errors, and extras
   * @throws java.lang.IllegalArgumentException when `value` is missing or is not a valid JSON schema string.
   */
  def analyze(df: DataFrame, rule: RuleDefinition): MetricResult = {
    val schemaJson = rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException("validate_schema requires a JSON schema as value"))

    import io.galileostd.sumeh.schema.SchemaDef
    import io.galileostd.sumeh.spark.schema.SparkSchemaValidator
    import upickle.default._

    val schemaMap = read[Map[String, ujson.Value]](schemaJson).map { case (k, v) => k -> ujsonToAny(v) }
    val schemaDef = SchemaDef.fromMap(schemaMap)
    val report    = SparkSchemaValidator.validate(df, schemaDef)

    MetricResult(
      metricType = "schema",
      field = Left("*"),
      value = if (report.passed) 1.0 else 0.0,
      totalRows = -1L, // schema checks inspect the catalog, not the data; no count() job here
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
