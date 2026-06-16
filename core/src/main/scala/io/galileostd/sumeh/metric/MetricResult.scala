package io.galileostd.sumeh.metric

import io.galileostd.sumeh.rule.{ ListValue, RuleValue }

/**
 * Output of an Analyzer — pure computation, no opinion.
 *
 * An Analyzer computes a metric on data:
 *   - CompletenessAnalyzer → null_count, completeness_rate
 *   - MeanAnalyzer → mean_value
 *   - PatternAnalyzer → matching/non-matching row ids
 *
 * Analyzers are PURE — same input always gives same output. Analyzers DON'T know about thresholds or rules.
 *
 * @param metricType
 *   "completeness", "mean", "pattern", "cardinality", etc
 * @param field
 *   Column name(s) analyzed
 * @param value
 *   Primary metric value
 * @param totalRows
 *   Total row count of the DataFrame
 * @param affectedRowIds
 *   Row indices that violate the constraint
 * @param metadata
 *   Extra context (null_count, distribution, etc)
 */
final case class MetricResult(
    metricType: String,
    field: Either[String, List[String]],
    value: Double,
    totalRows: Long,
    affectedRowIds: List[Long] = List.empty,
    metadata: Map[String, Any] = Map.empty
) {
  def fieldName: String = field.fold(identity, _.mkString(","))

  override def toString: String =
    s"MetricResult(type=$metricType, field=$fieldName, value=$value)"
}
