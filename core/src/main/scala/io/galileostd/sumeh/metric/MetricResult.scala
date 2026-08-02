package io.galileostd.sumeh.metric

/**
 * Output of an Analyzer — pure computation, no opinion.
 *
 * An Analyzer computes a metric on data (e.g. completeness → null_count, mean → mean_value). Analyzers are pure — the
 * same input always yields the same output — and know nothing about thresholds or rules. A `SparkConstraint` later
 * compares this metric against the rule's expectation to decide pass/fail.
 *
 * @param metricType
 *   The metric kind, e.g. `"completeness"`, `"mean"`, `"pattern"`, `"cardinality"`.
 * @param field
 *   The column name(s) analyzed — `Left` for one, `Right` for several.
 * @param value
 *   The primary metric value (usually a pass rate or an aggregation result).
 * @param totalRows
 *   Total row count of the DataFrame at analysis time.
 * @param affectedRowIds
 *   Row indices that violate the rule (row-level rules only).
 * @param metadata
 *   Extra context (null_count, distribution, condition, ...), keyed by name.
 */
final case class MetricResult(
    metricType: String,
    field: Either[String, List[String]],
    value: Double,
    totalRows: Long,
    affectedRowIds: List[Long] = List.empty,
    metadata: Map[String, Any] = Map.empty
) {

  /**
   * Flattened column name(s): a single name for `Left`, or a comma-joined string for `Right`.
   *
   * @return
   *   The column name, or comma-joined column names.
   */
  def fieldName: String = field.fold(identity, _.mkString(","))

  /**
   * Compact rendering of the metric.
   *
   * @return
   *   A string like `MetricResult(type=completeness, field=email, value=0.95)`.
   */
  override def toString: String =
    s"MetricResult(type=$metricType, field=$fieldName, value=$value)"
}
