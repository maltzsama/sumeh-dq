package io.galileostudio.sumeh.spark.constraint

import java.util.UUID

import io.galileostudio.sumeh.metric.MetricResult
import io.galileostudio.sumeh.rule.RuleDefinition
import io.galileostudio.sumeh.validation.{ ValidationLevel, ValidationResult, ValidationStatus }

// ============================================================================
// Base trait
// ============================================================================

trait SparkConstraint {
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult
}

// ============================================================================
// Completeness
// ============================================================================

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

object AggregationConstraint extends SparkConstraint {
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val expected = rule.value.collect {
      case io.galileostudio.sumeh.rule.DoubleValue(d) => d
      case io.galileostudio.sumeh.rule.LongValue(l)   => l.toDouble
    }

    val passed = expected.forall(exp => metric.value == exp)

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
