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
 * @param results All validation results from the run.
 * @param totalRows Total rows in the validated dataset.
 * @param executionTimeMs How long validation took.
 * @param engine Engine that produced this report (e.g. `"spark"`, `"flink"`).
 * @param errorMessage Top-level error if validation failed entirely.
 * @param timestamp When the report was generated.
 * @param dfValidated Engine-specific validated dataset wrapper, used by [[split]].
 * @param generatedSql SQL generated during validation, if applicable.
 * @param runId identifier shared by every row this run produces in `toDataFrame`,
 *              so results from one execution can be grouped in a metrics table.
 *              Generated per report; override with `copy(runId = ...)` to tie it to
 *              an orchestrator's job id.
 */
final case class ValidationReport[DF](
    results: List[ValidationResult],
    totalRows: Long,
    executionTimeMs: Double,
    engine: String,
    errorMessage: Option[String] = None,
    timestamp: LocalDateTime = LocalDateTime.now(java.time.ZoneOffset.UTC),
    dfValidated: Option[DF] = None,
    generatedSql: Option[String] = None,
    runId: String = java.util.UUID.randomUUID().toString
) {

  /**
   * Results whose status is PASS.
   *
   * @return The passing results.
   */
  def passed: List[ValidationResult] = results.filter(_.status == ValidationStatus.PASS)

  /**
   * Results whose status is FAIL.
   *
   * @return The failing results.
   */
  def failed: List[ValidationResult] = results.filter(_.status == ValidationStatus.FAIL)

  /**
   * Results whose status is ERROR.
   *
   * @return The errored results.
   */
  def errors: List[ValidationResult] = results.filter(_.status == ValidationStatus.ERROR)

  /**
   * Results whose status is SKIPPED.
   *
   * @return The skipped results.
   */
  def skipped: List[ValidationResult] = results.filter(_.status == ValidationStatus.SKIPPED)

  /**
   * Fraction of evaluated (non-skipped) validations that passed.
   *
   * Rules that were skipped (execute=false, wrong level, unsupported engine) neither pass nor fail and are excluded
   * from the denominator, so a stream with many skipped TABLE rules is not unfairly punished.
   *
   * @return `passed / evaluated` in `[0.0, 1.0]`, or `1.0` when there is nothing to evaluate.
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
   * @return A `(good, bad)` tuple of validated datasets.
   * @throws java.lang.IllegalStateException when no validated dataset is attached to this report.
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
   * @return The good rows.
   */
  def goodDf()(
      implicit splittable: Splittable[DF]
  ): DF = split()._1

  /**
   * Shortcut for [[split]]._2 — the bad (failing) dataset.
   *
   * @return The bad rows.
   */
  def badDf()(
      implicit splittable: Splittable[DF]
  ): DF = split()._2

  /**
   * Flat JSON-friendly map for dashboards / sinks / alerting.
   *
   * Includes run-level totals (`total_rows`, `passed`, `failed`, `errors`, `skipped`, `pass_rate`) and a per-rule
   * `validations` list. The `rule_id` field is the same value that appears in `_dq_errors[i].rule_id`, allowing a
   * failing row to be correlated back to the validation that flagged it. `fail_count` comes from the rule's
   * `metadata("fail_count")` (populated by the engines); it is `null` when the metric reported no violation count
   * (e.g. `validate_schema`), mirroring the `fail_count` column of `toDataFrame`.
   *
   * Note: unlike the Python implementation, the JVM report does not expose individual violating row ids. Materialising
   * them would require collecting every id to the driver per rule, which breaks the single-pass model. Use the `bad`
   * DataFrame from `split()` to inspect the rows, and `fail_count` for the count — which is the same number as
   * `len(violating_row_ids)` on the Python side.
   *
   * @return A serializable map describing the run.
   */
  def summary(): Map[String, Any] = Map(
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
          "rule_id"    -> r.id,
          "check_type" -> r.checkType,
          "field"      -> r.fieldName,
          "category"   -> r.category,
          "level"      -> r.level.toString,
          "status"     -> r.status.toString,
          "pass_rate"  -> r.passRate.map(java.lang.Double.valueOf(_)).orNull,
          "expected"   -> r.expectedValue.map(java.lang.Double.valueOf(_)).orNull,
          "actual"     -> r.actualValue.map(java.lang.Double.valueOf(_)).orNull,
          "message"    -> r.message.orNull,
          "fail_count" -> r.failCount.map(java.lang.Long.valueOf(_)).orNull
        )
    }
  )

  /**
   * Number of validation results.
   *
   * @return The result count.
   */
  def size: Int = results.size

  /**
   * Whether there are no validation results.
   *
   * @return `true` when `results` is empty.
   */
  def isEmpty: Boolean = results.isEmpty

  /**
   * Compact rendering of the run outcome.
   *
   * @return A string like `ValidationReport(3 rules, 1 failed, pass_rate=0.67)`.
   */
  override def toString: String =
    s"ValidationReport(${results.size} rules, ${failed.size} failed, pass_rate=${String.format(Locale.ROOT, "%.2f", Double.box(passRate))})"
}
