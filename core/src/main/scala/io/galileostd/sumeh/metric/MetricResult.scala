package io.galileostd.sumeh.metric

/**
 * Output of an Analyzer — pure computation, no opinion.
 *
 * An Analyzer computes a metric on data (e.g. CompletenessAnalyzer -> null_count, MeanAnalyzer -> mean_value).
 * Analyzers are pure — the same input always yields the same output — and don't know about thresholds or rules.
 *
 * Args: metricType: "completeness", "mean", "pattern", "cardinality", etc. field: Column name(s) analyzed. value:
 * Primary metric value. totalRows: Total row count of the DataFrame. affectedRowIds: Row indices that violate the
 * constraint. metadata: Extra context (null_count, distribution, etc.).
 */
final case class MetricResult(
    metricType: String,
    field: Either[String, List[String]],
    value: Double,
    totalRows: Long,
    affectedRowIds: List[Long] = List.empty,
    metadata: Map[String, Any] = Map.empty
) {

  /** Flattened column name(s): single name or comma-joined list. */
  def fieldName: String = field.fold(identity, _.mkString(","))

  override def toString: String =
    s"MetricResult(type=$metricType, field=$fieldName, value=$value)"
}
