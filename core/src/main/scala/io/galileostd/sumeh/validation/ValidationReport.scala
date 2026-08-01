package io.galileostd.sumeh.validation

import java.time.LocalDateTime
import java.util.Locale

import io.galileostd.sumeh.engine.Splittable

/**
 * Collection of [[ValidationResult]]s for a single validation run.
 *
 * Returned by engine `validate()` calls. Use [[split]] to separate good vs. bad rows (the Bifurcation pattern) and
 * [[summary]] for a lightweight JSON-friendly payload for dashboards, sinks, and alerting.
 *
 * Args: results: All validation results from the run. totalRows: Total rows in the validated dataset. executionTimeMs:
 * How long validation took. engine: Engine that produced this report (e.g. `"spark"`, `"flink"`). errorMessage:
 * Top-level error if validation failed entirely. timestamp: When the report was generated. dfValidated: Engine-specific
 * validated dataset wrapper, used by [[split]]. generatedSql: SQL generated during validation, if applicable.
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

  /**
   * Results whose status is PASS.
   *
   * Returns: The passing results.
   */
  def passed: List[ValidationResult] = results.filter(_.status == ValidationStatus.PASS)

  /**
   * Results whose status is FAIL.
   *
   * Returns: The failing results.
   */
  def failed: List[ValidationResult] = results.filter(_.status == ValidationStatus.FAIL)

  /**
   * Results whose status is ERROR.
   *
   * Returns: The errored results.
   */
  def errors: List[ValidationResult] = results.filter(_.status == ValidationStatus.ERROR)

  /**
   * Results whose status is SKIPPED.
   *
   * Returns: The skipped results.
   */
  def skipped: List[ValidationResult] = results.filter(_.status == ValidationStatus.SKIPPED)

  /**
   * Fraction of evaluated (non-skipped) validations that passed.
   *
   * Rules that were skipped (execute=false, wrong level, unsupported engine) neither pass nor fail and are excluded
   * from the denominator, so a stream with many skipped TABLE rules is not unfairly punished.
   *
   * Returns: `passed / evaluated` in `[0.0, 1.0]`, or `1.0` when there is nothing to evaluate.
   */
  def passRate: Double = {
    val evaluated = results.size - skipped.size
    if (evaluated == 0) 1.0
    else passed.size.toDouble / evaluated
  }

  /**
   * Splits the validated dataset into (good, bad).
   *
   * Delegates to the engine-specific [[io.galileostd.sumeh.engine.Splittable]] instance supplied implicitly, which
   * performs the split with no reprocessing.
   *
   * Returns: A `(good, bad)` tuple of validated datasets.
   *
   * Throws: IllegalStateException when no validated dataset is attached to this report.
   */
  def split()(
      implicit splittable: Splittable[DF]
  ): (DF, DF) =
    dfValidated match {
      case Some(df) => splittable.split(df)
      case None     => throw new IllegalStateException("No validated DataFrame available")
    }

  /**
   * Shortcut for [[split]]._1 — the good (passing) dataset.
   *
   * Returns: The good rows.
   */
  def goodDf()(
      implicit splittable: Splittable[DF]
  ): DF = split()._1

  /**
   * Shortcut for [[split]]._2 — the bad (failing) dataset.
   *
   * Returns: The bad rows.
   */
  def badDf()(
      implicit splittable: Splittable[DF]
  ): DF = split()._2

  /**
   * Flat JSON-friendly map for dashboards / sinks / alerting.
   *
   * Includes run-level totals (`total_rows`, `passed`, `failed`, `errors`, `skipped`, `pass_rate`) and a per-rule
   * `validations` list with status, measured vs. expected values, and a sample of violating row ids. `fail_count` comes
   * from the rule's `metadata("fail_count")` (populated by the engines); `violatingRowIds` is reserved and not yet
   * populated.
   *
   * Args: maxSampleIds: Maximum number of violating row ids to include per rule.
   *
   * Returns: A serializable map describing the run.
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
          "fail_count"           -> failCountOf(r),
          "sample_violating_ids" -> r.violatingRowIds.take(maxSampleIds)
        )
    }
  )

  /**
   * Number of validation results.
   *
   * Returns: The result count.
   */
  def size: Int = results.size

  /**
   * Whether there are no validation results.
   *
   * Returns: `true` when `results` is empty.
   */
  def isEmpty: Boolean = results.isEmpty

  /**
   * Compact rendering of the run outcome.
   *
   * Returns: A string like `ValidationReport(3 rules, 1 failed, pass_rate=0.67)`.
   */
  override def toString: String =
    s"ValidationReport(${results.size} rules, ${failed.size} failed, pass_rate=${String.format(Locale.ROOT, "%.2f", Double.box(passRate))})"

  /**
   * Number of failing rows for a result, read from its metadata.
   *
   * Engines report the count under `fail_count` (most rules) or `null_count`/`incomplete_count` (completeness rules);
   * `0` when absent.
   *
   * Args: r: The validation result.
   *
   * Returns: The failing-row count.
   */
  private def failCountOf(r: ValidationResult): Long =
    Seq("fail_count", "null_count", "incomplete_count")
      .flatMap(k => r.metadata.get(k))
      .headOption
      .map(_.toString.toLong)
      .getOrElse(0L)
}
