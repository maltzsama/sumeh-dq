package io.galileostd.sumeh.validation

import java.time.LocalDateTime
import java.util.UUID

import io.galileostd.sumeh.rule.RuleValue

/**
 * Output of a Constraint — compares a metric to the rule expectation. One result per rule executed.
 *
 * Args: id: Unique identifier. timestamp: When validation ran. ruleId: Rule identifier. level: ROW or TABLE. category:
 * "completeness", "uniqueness", etc. checkType: Rule type (e.g. "is_complete"). field: Column name(s) validated.
 * status: PASS, FAIL, ERROR, or SKIPPED. passRate: % of rows that passed (row-level only). expectedValue: What the rule
 * expected. actualValue: What was actually measured. violatingRowIds: Row indices that failed. message: Human-readable
 * explanation. metadata: Extra context.
 */
final case class ValidationResult(
    id: String = UUID.randomUUID().toString,
    timestamp: LocalDateTime = LocalDateTime.now(),
    ruleId: String = "",
    level: ValidationLevel = ValidationLevel.ROW,
    category: String = "unknown",
    checkType: String = "",
    field: Either[String, List[String]] = Left(""),
    status: ValidationStatus = ValidationStatus.ERROR,
    passRate: Option[Double] = None,
    expectedValue: Option[RuleValue] = None,
    actualValue: Option[Double] = None,
    violatingRowIds: List[Long] = List.empty,
    message: Option[String] = None,
    metadata: Map[String, Any] = Map.empty
) {

  /** Flattened column name(s): single name or comma-joined list. */
  def fieldName: String = field.fold(identity, _.mkString(","))

  override def toString: String =
    s"ValidationResult($checkType on $fieldName: $status)"
}

/** Companion with result constructors. */
object ValidationResult {

  /**
   * Result for a rule that was not executed (execute=false, wrong level, unsupported engine, etc).
   *
   * Args: checkType: The rule type. field: Column name(s). level: The rule's level. category: The rule's category.
   * reason: Why the rule was skipped.
   *
   * Returns: A SKIPPED ValidationResult.
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
