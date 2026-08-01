package io.galileostd.sumeh.spark.constraint

import java.util.UUID

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationResult, ValidationStatus }

// ============================================================================
// Base trait
// ============================================================================

/**
 * Compares a `MetricResult` against the rule's expectation and produces a `ValidationResult`.
 *
 * Constraints are the only component that knows about thresholds and pass/fail semantics; analyzers stay pure. Each
 * rule family has a constraint (completeness, uniqueness, generic, aggregation, schema).
 */
trait SparkConstraint {

  /**
   * Checks a metric against the rule.
   *
   * Args: metric: The computed metric. rule: The rule with its threshold/value.
   *
   * Returns: The `ValidationResult`.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult
}

// ============================================================================
// Completeness
// ============================================================================

/**
 * Constraint for completeness rules (`is_complete`, `are_complete`).
 *
 * Passes when the measured completeness pass rate meets `rule.threshold`.
 */
object CompletenessConstraint extends SparkConstraint {

  /**
   * Compares the completeness metric against the rule's threshold.
   *
   * Args: metric: A completeness metric (value = non-null fraction). rule: The rule carrying the threshold.
   *
   * Returns: PASS when `metric.value >= rule.threshold`, otherwise FAIL with a percentage message.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.value >= rule.threshold
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.ROW,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      passRate = Some(metric.value),
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else Some(s"Completeness ${f"${metric.value * 100}%.2f"}% below threshold ${f"${rule.threshold * 100}%.2f"}%"),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Uniqueness
// ============================================================================

/**
 * Constraint for uniqueness rules (`is_unique`, `are_unique`, `is_primary_key`, ...).
 *
 * Passes when the measured non-duplicate fraction meets `rule.threshold`.
 */
object UniquenessConstraint extends SparkConstraint {

  /**
   * Compares the uniqueness metric against the rule's threshold.
   *
   * Args: metric: A uniqueness metric (value = non-duplicate fraction). rule: The rule carrying the threshold.
   *
   * Returns: PASS when `metric.value >= rule.threshold`, otherwise FAIL with a percentage message.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.value >= rule.threshold
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.ROW,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      passRate = Some(metric.value),
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else Some(s"Uniqueness ${f"${metric.value * 100}%.2f"}% below threshold ${f"${rule.threshold * 100}%.2f"}%"),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Generic (comparison, membership, pattern, date)
// ============================================================================

/**
 * Constraint for comparison, membership, pattern, date, and SQL rules.
 *
 * Passes when the measured pass rate meets `rule.threshold`.
 */
object GenericConstraint extends SparkConstraint {

  /**
   * Compares a pass-rate metric against the rule's threshold.
   *
   * Args: metric: Any pass-rate metric (value in `[0.0, 1.0]`). rule: The rule carrying the threshold.
   *
   * Returns: PASS when `metric.value >= rule.threshold`, otherwise FAIL with a percentage message.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.value >= rule.threshold
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.ROW,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      passRate = Some(metric.value),
      actualValue = Some(metric.value),
      message = if (passed) None
      else
        Some(
          s"${rule.checkType} pass rate ${f"${metric.value * 100}%.2f"}% below threshold ${f"${rule.threshold * 100}%.2f"}%"
        ),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Aggregation (TABLE level)
// ============================================================================

/**
 * Constraint for TABLE-level aggregations.
 *
 * Compares the measured aggregation to the rule's expected `value`, passing when they are equal (or, when
 * `rule.threshold > 0`, within a relative tolerance `|actual - expected| / expected <= threshold`). An expected value
 * of `0` requires exact equality.
 */
object AggregationConstraint extends SparkConstraint {

  /**
   * Compares an aggregation metric against the rule's expected value.
   *
   * Args: metric: An aggregation metric (value = the aggregated number). rule: The rule carrying the expected `value`
   * and relative `threshold`.
   *
   * Returns: PASS when the measured value matches the expectation within tolerance, otherwise FAIL.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val expected = rule.value.collect {
      case io.galileostd.sumeh.rule.DoubleValue(d) => d
      case io.galileostd.sumeh.rule.LongValue(l)   => l.toDouble
    }

    val passed = expected.forall {
      exp =>
        if (exp == 0) metric.value == exp
        else if (rule.threshold > 0) Math.abs(metric.value - exp) / exp <= rule.threshold
        else metric.value == exp
    }

    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.TABLE,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else Some(s"${rule.checkType}: expected ${expected.getOrElse("?")} but got ${metric.value}"),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Schema (TABLE level)
// ============================================================================

/**
 * Constraint for `validate_schema`.
 *
 * Passes when the schema metric's `passed` metadata flag is `true`.
 */
object SchemaConstraint extends SparkConstraint {

  /**
   * Compares the schema metric's `passed` flag.
   *
   * Args: metric: A schema metric (value `1.0`/`0.0`, `passed` flag in metadata). rule: The rule being checked.
   *
   * Returns: PASS when the schema report passed, otherwise FAIL listing the type errors.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.metadata.get("passed").exists(_.asInstanceOf[Boolean])
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.TABLE,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else Some(s"validate_schema: schema does not match contract (${metric.metadata.get("type_errors")})"),
      metadata = metric.metadata
    )
  }
}
