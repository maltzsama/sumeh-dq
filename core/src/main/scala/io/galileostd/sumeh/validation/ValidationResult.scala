package io.galileostd.sumeh.validation

import java.time.LocalDateTime
import java.util.UUID

import io.galileostd.sumeh.rule.RuleValue

/**
 * Output of a Constraint — compares a metric to the rule expectation.
 *
 * One result per rule executed. It carries the rule identity, the outcome (`status`), the measured vs. expected values,
 * and a human-readable message, so downstream consumers (dashboards, alerting, sinks) can act on it without re-deriving
 * the comparison.
 *
 * Args: id: Unique identifier of this validation result, generated once per execution. This is the same value that
 * appears in `_dq_errors[i].result_id`, and it is how a failing row is correlated back to the validation that flagged
 * it. Not stable across executions. timestamp: When the validation ran. level: ROW or TABLE. category: The rule
 * category (e.g. `"completeness"`, `"uniqueness"`). checkType: The rule type (e.g. `"is_complete"`). field: Column
 * name(s) validated. status: PASS, FAIL, ERROR, or SKIPPED. passRate: Percentage of rows that passed (row-level rules
 * only). expectedValue: What the rule expected. actualValue: What was actually measured. message: Human-readable
 * explanation (e.g. why a rule failed). metadata: Extra context from the metric.
 */
final case class ValidationResult(
    id: String = UUID.randomUUID().toString,
    timestamp: LocalDateTime = LocalDateTime.now(),
    level: ValidationLevel = ValidationLevel.ROW,
    category: String = "unknown",
    checkType: String = "",
    field: Either[String, List[String]] = Left(""),
    status: ValidationStatus = ValidationStatus.ERROR,
    passRate: Option[Double] = None,
    expectedValue: Option[RuleValue] = None,
    actualValue: Option[Double] = None,
    message: Option[String] = None,
    metadata: Map[String, Any] = Map.empty
) {

  /**
   * Flattened column name(s): a single name for `Left`, or a comma-joined string for `Right`.
   *
   * Returns: The column name, or comma-joined column names.
   */
  def fieldName: String = field.fold(identity, _.mkString(","))

  /**
   * Compact rendering of the result outcome.
   *
   * Returns: A string like `ValidationResult(is_complete on email: PASS)`.
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
   * Args: checkType: The rule type. field: Column name(s). level: The rule's level. category: The rule's category.
   * reason: Why the rule was skipped.
   *
   * Returns: A SKIPPED [[ValidationResult]] whose message starts with `Skipped: `.
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
