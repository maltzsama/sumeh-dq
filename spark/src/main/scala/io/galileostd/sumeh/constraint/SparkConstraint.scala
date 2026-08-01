package io.galileostd.sumeh.spark.constraint

import java.util.UUID

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationResult, ValidationStatus }

// ============================================================================
// Base trait
// ============================================================================

/**
 * Compares a MetricResult against the rule's expectation and produces a ValidationResult.
 *
 * Constraints are the only place that knows about thresholds and pass/fail semantics; analyzers stay pure.
 */
trait SparkConstraint {

  /**
   * Checks a metric against the rule.
   *
   * Args: metric: The computed metric. rule: The rule with its threshold/value.
   *
   * Returns: The ValidationResult.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult
}

// ============================================================================
// Completeness
// ============================================================================

/** Constraint for completeness rules (is_complete, are_complete). */
object CompletenessConstraint extends SparkConstraint {
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

/** Constraint for uniqueness rules (is_unique, are_unique, is_primary_key, ...). */
object UniquenessConstraint extends SparkConstraint {
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

/** Constraint for comparison, membership, pattern, date, and SQL rules. */
object GenericConstraint extends SparkConstraint {
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
 * Constraint for TABLE-level aggregations — compares the metric to `value` within a relative threshold.
 */
object AggregationConstraint extends SparkConstraint {
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

/** Constraint for validate_schema — passes when the SchemaReport passed. */
object SchemaConstraint extends SparkConstraint {
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
