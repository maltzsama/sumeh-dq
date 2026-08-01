package io.galileostd.sumeh.validation

import java.time.LocalDateTime

import io.galileostd.sumeh.engine.Splittable

/**
 * Collection of ValidationResults for a single validation run. Returned by engine validate() calls.
 *
 * Use split() to separate good vs bad rows (Bifurcation) and summary() for a lightweight JSON-friendly payload.
 *
 * Args: results: All validation results. totalRows: Total rows in the DataFrame. executionTimeMs: How long validation
 * took. engine: Engine that produced this report. errorMessage: Top-level error if validation failed entirely.
 * timestamp: When the report was generated. dfValidated: Engine-specific validated DataFrame wrapper. generatedSql: SQL
 * generated during validation (if applicable).
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

  /** Results whose status is PASS. */
  def passed: List[ValidationResult] = results.filter(_.status == ValidationStatus.PASS)

  /** Results whose status is FAIL. */
  def failed: List[ValidationResult] = results.filter(_.status == ValidationStatus.FAIL)

  /** Results whose status is ERROR. */
  def errors: List[ValidationResult] = results.filter(_.status == ValidationStatus.ERROR)

  /** Results whose status is SKIPPED. */
  def skipped: List[ValidationResult] = results.filter(_.status == ValidationStatus.SKIPPED)

  /**
   * Fraction of evaluated (non-skipped) validations that passed.
   *
   * Rules that were skipped (execute=false, wrong level, unsupported engine) neither pass nor fail and are excluded.
   * Returns 1.0 when there is nothing to evaluate.
   */
  def passRate: Double = {
    val evaluated = results.size - skipped.size
    if (evaluated == 0) 1.0
    else passed.size.toDouble / evaluated
  }

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

  /**
   * Flat JSON-friendly map for dashboards / sinks / alerting.
   *
   * Args: maxSampleIds: Maximum number of violating row ids to include per rule.
   *
   * Returns: A map with run-level totals, pass rate, and per-rule validation details.
   */
  def summary(maxSampleIds: Int = 100): Map[String, Any] = Map(
    "timestamp"         -> timestamp.toString,
    "engine"            -> engine,
    "total_rows"        -> totalRows,
    "execution_time_ms" -> executionTimeMs,
    "total_validations" -> results.size,
    "passed"            -> passed.size,
    "failed"            -> failed.size,
    "errors"            -> errors.size,
    "skipped"           -> skipped.size,
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

  /** Number of validation results. */
  def size: Int = results.size

  /** True when there are no validation results. */
  def isEmpty: Boolean = results.isEmpty

  override def toString: String =
    s"ValidationReport(${results.size} rules, ${failed.size} failed, pass_rate=${f"$passRate%.2f"})"
}
