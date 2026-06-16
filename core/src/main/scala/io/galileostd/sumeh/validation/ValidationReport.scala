package io.galileostd.sumeh.validation

import java.time.LocalDateTime

import io.galileostd.sumeh.engine.Splittable

// ValidationReport
/**
 * Collection of ValidationResults for a single validation run. Returned by engine validate() calls.
 *
 * Bifurcation: use split() to separate good vs bad rows. Use summary() for lightweight JSON payload (e.g. Deletron
 * sink).
 *
 * @param results
 *   All validation results
 * @param totalRows
 *   Total rows in the DataFrame
 * @param executionTimeMs
 *   How long validation took
 * @param engine
 *   Engine that produced this report
 * @param errorMessage
 *   Top-level error if validation failed entirely
 * @param timestamp
 *   When the report was generated
 * @param dfValidated
 *   Engine-specific validated DataFrame wrapper
 * @param generatedSql
 *   SQL generated during validation (if applicable)
 */
final case class ValidationReport[DF](
    results: List[ValidationResult],
    totalRows: Long,
    executionTimeMs: Double,
    engine: String,
    errorMessage: Option[String] = None,
    timestamp: LocalDateTime = LocalDateTime.now(),
    dfValidated: Option[DF] = None,
    generatedSql: Option[String] = None
) {
  def passed: List[ValidationResult] = results.filter(_.status == ValidationStatus.PASS)
  def failed: List[ValidationResult] = results.filter(_.status == ValidationStatus.FAIL)
  def errors: List[ValidationResult] = results.filter(_.status == ValidationStatus.ERROR)

  def passRate: Double =
    if (results.isEmpty) 1.0
    else passed.size.toDouble / results.size

  /**
   * Split validated DataFrame into (good, bad). Delegates to the engine-specific wrapper.
   */
  def split()(
      implicit splittable: Splittable[DF]
  ): (DF, DF) =
    dfValidated match {
      case Some(df) => splittable.split(df)
      case None     => throw new IllegalStateException("No validated DataFrame available")
    }

  /** Shortcut for split()._1 */
  def goodDf()(
      implicit splittable: Splittable[DF]
  ): DF = split()._1

  /** Shortcut for split()._2 */
  def badDf()(
      implicit splittable: Splittable[DF]
  ): DF = split()._2

  def summary(maxSampleIds: Int = 100): Map[String, Any] = Map(
    "timestamp"         -> timestamp.toString,
    "engine"            -> engine,
    "total_rows"        -> totalRows,
    "execution_time_ms" -> executionTimeMs,
    "total_validations" -> results.size,
    "passed"            -> passed.size,
    "failed"            -> failed.size,
    "errors"            -> errors.size,
    "pass_rate"         -> passRate,
    "validations" -> results.map {
      r =>
        Map(
          "rule_id"              -> r.ruleId,
          "check_type"           -> r.checkType,
          "field"                -> r.fieldName,
          "category"             -> r.category,
          "level"                -> r.level.toString,
          "status"               -> r.status.toString,
          "pass_rate"            -> r.passRate.map(java.lang.Double.valueOf(_)).orNull,
          "expected"             -> r.expectedValue.orNull,
          "actual"               -> r.actualValue.map(java.lang.Double.valueOf(_)).orNull,
          "message"              -> r.message.orNull,
          "fail_count"           -> r.violatingRowIds.size,
          "sample_violating_ids" -> r.violatingRowIds.take(maxSampleIds)
        )
    }
  )

  def size: Int        = results.size
  def isEmpty: Boolean = results.isEmpty

  override def toString: String =
    s"ValidationReport(${results.size} rules, ${failed.size} failed, pass_rate=${f"$passRate%.2f"})"
}
