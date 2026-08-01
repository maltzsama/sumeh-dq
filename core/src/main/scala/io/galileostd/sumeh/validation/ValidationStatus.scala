package io.galileostd.sumeh.validation

/**
 * Final status of a single validation result.
 *
 *   - PASS: the metric satisfied the rule expectation.
 *   - FAIL: the metric violated the rule.
 *   - ERROR: the rule could not be evaluated (unknown field, invalid config, runtime error).
 *   - SKIPPED: the rule was intentionally not executed (see ValidationResult.skipped).
 */
sealed trait ValidationStatus

/** Concrete ValidationStatus values. */
object ValidationStatus {

  /** The rule passed. */
  case object PASS extends ValidationStatus {
    override def toString = "PASS"
  }

  /** The rule failed. */
  case object FAIL extends ValidationStatus {
    override def toString = "FAIL"
  }

  /** The rule could not be evaluated. */
  case object ERROR extends ValidationStatus {
    override def toString = "ERROR"
  }

  /** The rule was intentionally not executed. */
  case object SKIPPED extends ValidationStatus {
    override def toString = "SKIPPED"
  }

}
