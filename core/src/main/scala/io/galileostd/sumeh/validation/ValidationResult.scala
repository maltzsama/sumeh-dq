package io.galileostd.sumeh.validation

import java.time.LocalDateTime
import java.util.UUID

import io.galileostd.sumeh.rule.RuleValue

// ValidationResult
/**
 * Output of a Constraint — compares metric to rule expectation. One ValidationResult per rule executed.
 *
 * @param id
 *   Unique identifier
 * @param timestamp
 *   When validation ran
 * @param ruleId
 *   Rule identifier
 * @param level
 *   ROW or TABLE
 * @param category
 *   "completeness", "uniqueness", etc
 * @param checkType
 *   Rule type (e.g. "is_complete")
 * @param field
 *   Column name(s) validated
 * @param status
 *   PASS, FAIL, or ERROR
 * @param passRate
 *   % of rows that passed (row-level only)
 * @param expectedValue
 *   What the rule expected
 * @param actualValue
 *   What was actually measured
 * @param violatingRowIds
 *   Row indices that failed
 * @param message
 *   Human-readable explanation
 * @param metadata
 *   Extra context
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
  def fieldName: String = field.fold(identity, _.mkString(","))

  override def toString: String =
    s"ValidationResult($checkType on $fieldName: $status)"
}

object ValidationResult {

  /** Result for a rule that was not executed (execute=false, wrong level, unsupported engine, etc). */
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
