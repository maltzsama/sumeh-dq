package io.galileostd.sumeh.validation

import java.time.LocalDateTime
import java.util.UUID

/**
 * Output of a Constraint — compares a metric to the rule expectation.
 *
 * One result per rule executed. It carries the rule identity, the outcome (`status`), the measured vs. expected values,
 * and a human-readable message, so downstream consumers (dashboards, alerting, sinks) can act on it without re-deriving
 * the comparison.
 *
 * @param id
 *   Unique identifier for the result, generated once per execution. This is the value that appears in
 *   `_dq_errors[i].rule_id` and in `summary()("validations")(i)("rule_id")` — it is how a failing row is correlated
 *   back to the validation that flagged it. Not stable across executions; do not use as a time-series key.
 * @param category
 *   Rule category (e.g. `"completeness"`, `"uniqueness"`).
 * @param checkType
 *   The rule type (e.g. `"is_complete"`).
 * @param field
 *   Column name(s) validated.
 * @param status
 *   PASS, FAIL, ERROR, or SKIPPED.
 * @param passRate
 *   Percentage of rows that passed (row-level rules only).
 * @param expectedValue
 *   What the rule expected.
 * @param actualValue
 *   What was actually measured.
 * @param message
 *   Human-readable explanation (e.g. why a rule failed).
 * @param metadata
 *   Extra context from the metric.
 */
final case class ValidationResult(
    id: String = UUID.randomUUID().toString,
    timestamp: LocalDateTime = LocalDateTime.now(java.time.ZoneOffset.UTC),
    level: ValidationLevel = ValidationLevel.ROW,
    category: String = "unknown",
    checkType: String = "",
    field: Either[String, List[String]] = Left(""),
    status: ValidationStatus = ValidationStatus.ERROR,
    passRate: Option[Double] = None,
    expectedValue: Option[Double] = None,
    actualValue: Option[Double] = None,
    message: Option[String] = None,
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
   * Compact rendering of the result outcome.
   *
   * @return
   *   A string like `ValidationResult(is_complete on email: PASS)`.
   */
  override def toString: String =
    s"ValidationResult($checkType on $fieldName: $status)"
}

/**
 * Companion with result constructors.
 */
object ValidationResult {

  /**
   * Result for a rule that was not executed.
   *
   * A rule is skipped when `execute=false`, when it targets the wrong level, or when the engine does not support it.
   * Skipped rules never count as pass or fail — they are reported so the pipeline stays honest ("no silent passes").
   *
   * @param checkType
   *   The rule type.
   * @param field
   *   Column name(s).
   * @param level
   *   The rule's level.
   * @param category
   *   The rule's category.
   * @param reason
   *   Why the rule was skipped.
   * @return
   *   A SKIPPED [[ValidationResult]] whose message starts with `Skipped: `.
   */
  def skipped(
      checkType: String,
      field: Either[String, List[String]],
      level: ValidationLevel,
      category: String,
      reason: String
  ): ValidationResult =
    ValidationResult(
      checkType = checkType,
      field = field,
      level = level,
      category = category,
      status = ValidationStatus.SKIPPED,
      message = Some(s"Skipped: $reason")
    )
}
