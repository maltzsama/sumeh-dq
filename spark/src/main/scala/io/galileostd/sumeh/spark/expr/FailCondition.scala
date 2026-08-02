package io.galileostd.sumeh.spark.expr

import io.galileostd.sumeh.rule._
import io.galileostd.sumeh.spark.DateExpr
import org.apache.spark.sql.{ functions => F, Column }

/**
 * Column expression that is `true` for rows that *violate* the rule.
 *
 * Single source of truth: analyzers use it to count violations, the validator uses it to annotate rows in `_dq_errors`.
 * Covers only ROW-level rules.
 */
private[spark] object FailCondition {

  /**
   * Builds the fail-condition column expression for the given rule.
   *
   * Each case yields a Boolean column that is `true` exactly when the row violates the rule. Uniqueness checks use a
   * windowed count; `validate_schema` is not covered (TABLE-level, never reaches this path).
   *
   * Trust boundary: the `satisfies` case compiles the rule's `value` as Spark SQL via `F.expr`, so that value is
   * executed during validation. Only pass rules from sources you trust (your own config, not end-user input).
   *
   * @param rule
   *   The rule whose violation is tested.
   * @return
   *   A Boolean column — `true` when the row fails the rule.
   * @throws java.lang.IllegalArgumentException
   *   when no condition is defined for `rule.checkType`.
   */
  def apply(rule: RuleDefinition): Column = {
    val field = rule.field.fold(identity, _.head)

    rule.checkType match {
      // ---- Completeness -------------------------------------------------------
      case "is_complete" | "are_complete" =>
        val fields = rule.field.fold(List(_), identity)
        fields.map(f => F.col(f).isNull).reduce(_ || _)

      // ---- Uniqueness (windowed — analyzers cannot use this in aggregation) ---
      case "is_unique" | "are_unique" | "is_primary_key" | "is_composite_key" =>
        val fields = rule.field.fold(List(_), identity)
        val w      = org.apache.spark.sql.expressions.Window.partitionBy(fields.map(F.col): _*)
        F.count(F.lit(1)).over(w) > 1

      // ---- Comparison ---------------------------------------------------------
      case "is_equal"        => F.col(field) =!= F.lit(requireValue(rule, "is_equal requires a value"))
      case "is_greater_than" => F.col(field) <= F.lit(requireValue(rule, "is_greater_than requires a value"))
      case "is_less_than"    => F.col(field) >= F.lit(requireValue(rule, "is_less_than requires a value"))
      case "is_greater_or_equal_than" =>
        F.col(field) < F.lit(requireValue(rule, "is_greater_or_equal_than requires a value"))
      case "is_less_or_equal_than" => F.col(field) > F.lit(requireValue(rule, "is_less_or_equal_than requires a value"))
      case "is_positive"           => F.col(field) <= 0
      case "is_negative"           => F.col(field) >= 0
      case "is_in_millions"        => F.col(field) < 1000000L
      case "is_in_billions"        => F.col(field) < 1000000000L

      // ---- Between ------------------------------------------------------------
      case "is_between" =>
        val (lo, hi) = requirePair(rule, "is_between requires value=[min, max]")
        (F.col(field) < F.lit(RuleValue.toAny(lo))) || (F.col(field) > F.lit(RuleValue.toAny(hi)))

      // ---- Column comparison --------------------------------------------------
      case "is_equal_than" =>
        val other = requireString(rule, "is_equal_than requires a column name as value")
        F.col(field) =!= F.col(other)

      // ---- Membership ---------------------------------------------------------
      case "is_contained_in" | "is_in" =>
        val vals = requireList(rule, "is_contained_in requires a list of values").map(RuleValue.toAny)
        !F.col(field).isin(vals: _*)

      case "not_contained_in" | "not_in" =>
        val vals = requireList(rule, "not_contained_in requires a list of values").map(RuleValue.toAny)
        F.col(field).isin(vals: _*)

      // ---- Pattern ------------------------------------------------------------
      case "has_pattern" =>
        !F.col(field).rlike(requireString(rule, "has_pattern requires a regex in 'value'"))
      case "is_legit" =>
        F.col(field).isNull || (F.trim(F.col(field)) === "")

      // ---- Date ---------------------------------------------------------------
      case "all_date_checks"               => F.col(field).isNotNull && DateExpr.safeToDate(F.col(field)).isNull
      case "is_today"                      => DateExpr.safeToDate(F.col(field)) =!= F.current_date()
      case "is_t_minus_1" | "is_yesterday" => DateExpr.safeToDate(F.col(field)) =!= F.date_sub(F.current_date(), 1)
      case "is_t_minus_2"                  => DateExpr.safeToDate(F.col(field)) =!= F.date_sub(F.current_date(), 2)
      case "is_t_minus_3"                  => DateExpr.safeToDate(F.col(field)) =!= F.date_sub(F.current_date(), 3)
      case "is_past_date"                  => DateExpr.safeToDate(F.col(field)) >= F.current_date()
      case "is_future_date"                => DateExpr.safeToDate(F.col(field)) <= F.current_date()
      case "is_on_weekday"                 => F.dayofweek(DateExpr.safeToDate(F.col(field))).isin(1, 7)
      case "is_on_weekend"                 => !F.dayofweek(DateExpr.safeToDate(F.col(field))).isin(1, 7)
      case "is_on_monday"                  => F.dayofweek(DateExpr.safeToDate(F.col(field))) =!= 2
      case "is_on_tuesday"                 => F.dayofweek(DateExpr.safeToDate(F.col(field))) =!= 3
      case "is_on_wednesday"               => F.dayofweek(DateExpr.safeToDate(F.col(field))) =!= 4
      case "is_on_thursday"                => F.dayofweek(DateExpr.safeToDate(F.col(field))) =!= 5
      case "is_on_friday"                  => F.dayofweek(DateExpr.safeToDate(F.col(field))) =!= 6
      case "is_on_saturday"                => F.dayofweek(DateExpr.safeToDate(F.col(field))) =!= 7
      case "is_on_sunday"                  => F.dayofweek(DateExpr.safeToDate(F.col(field))) =!= 1

      case "is_date_between" =>
        val (start, end) = requirePair(rule, "is_date_between requires value=[start, end]") match {
          case (StringValue(s), StringValue(e)) => (s, e)
          case _ =>
            throw new IllegalArgumentException("is_date_between requires [start, end] date strings")
        }
        val dc = DateExpr.safeToDate(F.col(field))
        (dc < F.to_date(F.lit(start))) || (dc > F.to_date(F.lit(end)))

      case "is_date_after" =>
        val target = requireString(rule, "is_date_after requires a date value")
        DateExpr.safeToDate(F.col(field)) <= F.to_date(F.lit(target))

      case "is_date_before" =>
        val target = requireString(rule, "is_date_before requires a date value")
        DateExpr.safeToDate(F.col(field)) >= F.to_date(F.lit(target))

      case "validate_date_format" =>
        val format = requireString(rule, "validate_date_format requires a format string as value")
        F.try_to_timestamp(F.col(field), F.lit(format)).isNull && F.col(field).isNotNull

      // ---- SQL ----------------------------------------------------------------
      case "satisfies" =>
        !F.expr(requireString(rule, "satisfies requires a SQL condition as value"))

      case other =>
        throw new IllegalArgumentException(s"No fail condition defined for: $other")
    }
  }

  // ---- Helpers exposed for analyzers to extract metadata values --------------

  /**
   * Extracts a string value from the rule, throwing when absent.
   *
   * @param rule
   *   The rule whose value to extract.
   * @param msg
   *   The error message when the value is absent.
   * @return
   *   The `StringValue` contents.
   * @throws java.lang.IllegalArgumentException
   *   when `value` is missing or is not a `StringValue`.
   */
  private[spark] def requireString(rule: RuleDefinition, msg: String): String =
    rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException(msg))

  /**
   * Extracts a list value from the rule, throwing when absent.
   *
   * @param rule
   *   The rule whose value to extract.
   * @param msg
   *   The error message when the value is absent.
   * @return
   *   The `ListValue` items.
   * @throws java.lang.IllegalArgumentException
   *   when `value` is missing or is not a `ListValue`.
   */
  private[spark] def requireList(rule: RuleDefinition, msg: String): List[RuleValue] =
    rule.value
      .collect { case ListValue(items) => items }
      .getOrElse(throw new IllegalArgumentException(msg))

  /**
   * Extracts a pair `(lo, hi)` from the rule's list value, throwing when absent.
   *
   * @param rule
   *   The rule whose value to extract.
   * @param msg
   *   The error message when the value is absent.
   * @return
   *   The two items of the `ListValue`.
   * @throws java.lang.IllegalArgumentException
   *   when `value` is missing or is not a two-element `ListValue`.
   */
  private[spark] def requirePair(rule: RuleDefinition, msg: String): (RuleValue, RuleValue) =
    rule.value match {
      case Some(ListValue(lo :: hi :: Nil)) => (lo, hi)
      case _                                => throw new IllegalArgumentException(msg)
    }

  /**
   * Extracts the rule value converted to a plain JVM literal via `RuleValue.toAny`, throwing when absent.
   *
   * @param rule
   *   The rule whose value to extract.
   * @param msg
   *   The error message when the value is absent.
   * @return
   *   The plain JVM literal.
   * @throws java.lang.IllegalArgumentException
   *   when `value` is absent.
   */
  private[spark] def requireValue(rule: RuleDefinition, msg: String): Any =
    rule.value.map(RuleValue.toAny).getOrElse(throw new IllegalArgumentException(msg))
}
