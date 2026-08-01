package io.galileostd.sumeh.flink.internal

import java.time.format.DateTimeFormatter
import java.time.LocalDate

import io.galileostd.sumeh.rule.{ DoubleValue, ListValue, LongValue, RuleDefinition, StringValue }
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{ Collector, OutputTag }

/**
 * Flink ProcessFunction that evaluates each record against the rules.
 *
 * Enriches every record with `_dq_errors` and `_dq_skipped` fields, routes good records to the good side output and bad
 * records to the error side output, and also emits the enriched record on the main output. Evaluation is stateless —
 * one record at a time.
 *
 * Args: rules: Rules to evaluate. goodTag: Side-output tag for records that pass every rule. errorTag: Side-output tag
 * for records with at least one error.
 */
private[flink] class DQProcessFunction(
    rules: Seq[RuleDefinition],
    goodTag: OutputTag[Row],
    errorTag: OutputTag[Row]
) extends ProcessFunction[Row, Row] {

  /** Evaluates one record, routes it to the right side output, and emits the enriched row. */
  override def processElement(
      row: Row,
      ctx: ProcessFunction[Row, Row]#Context,
      out: Collector[Row]
  ): Unit = {

    val fieldCount = row.getArity
    val names      = row.getFieldNames(true).toArray().map(_.asInstanceOf[String])
    val values     = (0 until fieldCount).map(i => names(i) -> row.getField(i)).toMap

    val (errors, skipped) = DQProcessFunction.evaluate(values, rules)

    val enriched = Row.withNames()
    names.foreach(n => enriched.setField(n, values(n)))
    enriched.setField("_dq_errors", errors.mkString("|"))
    enriched.setField("_dq_skipped", skipped.mkString("|"))

    if (errors.isEmpty) ctx.output(goodTag, enriched)
    else ctx.output(errorTag, enriched)

    out.collect(enriched)
  }
}

/** Companion with the pure, cluster-free rule evaluation logic. */
private[flink] object DQProcessFunction {

  /**
   * Pure row-level evaluation of a single record: returns (errors, skippedReasons). Kept free of Flink runtime types so
   * the rule logic can be unit tested without a cluster.
   */
  private[flink] def evaluate(
      values: Map[String, Any],
      rules: Seq[RuleDefinition]
  ): (List[String], List[String]) = {
    val errors  = scala.collection.mutable.ListBuffer[String]()
    val skipped = scala.collection.mutable.ListBuffer[String]()

    for (rule <- rules)
      if (!rule.isApplicableForLevel("ROW")) {
        skipped += s"${rule.checkType}:TABLE-level rules are not supported in streaming"
      } else {
        rule.skipReason("ROW", "flink-streaming") match {
          case Some(reason) => skipped += s"${rule.checkType}:$reason"
          case None =>
            try
              if (!checkRule(values, rule)) errors += buildErrorMessage(rule)
            catch {
              case e: Exception => errors += s"ERROR[${rule.checkType}]: ${e.getMessage}"
            }
        }
      }

    (errors.toList, skipped.toList)
  }

  // -------------------------------------------------------------------------
  // Rule evaluation — pure row-level, no aggregation
  // -------------------------------------------------------------------------

  /** Checks one row against a single rule — pure row-level logic, no aggregation. */
  private def checkRule(values: Map[String, Any], rule: RuleDefinition): Boolean = {
    val field     = rule.field.fold(identity, _.head)
    val rawValue  = values.getOrElse(field, null)
    val checkType = rule.checkType

    if (
      rawValue == null &&
      checkType != "is_complete" &&
      checkType != "are_complete" &&
      checkType != "is_legit" &&
      checkType != "validate_date_format"
    )
      return true // null values skip non-completeness checks (consistent with Spark)

    checkType match {
      // Completeness
      case "is_complete" | "are_complete" =>
        val fields = rule.field.fold(List(_), identity)
        fields.forall(f => values.getOrElse(f, null) != null)

      // Comparison
      case "is_positive"    => toDouble(rawValue) > 0
      case "is_negative"    => toDouble(rawValue) < 0
      case "is_in_millions" => toDouble(rawValue) >= 1000000L
      case "is_in_billions" => toDouble(rawValue) >= 1000000000L

      case "is_equal" =>
        toDouble(rawValue) == ruleValueToDouble(rule.value)
      case "is_greater_than" =>
        toDouble(rawValue) > ruleValueToDouble(rule.value)
      case "is_less_than" =>
        toDouble(rawValue) < ruleValueToDouble(rule.value)
      case "is_greater_or_equal_than" =>
        toDouble(rawValue) >= ruleValueToDouble(rule.value)
      case "is_less_or_equal_than" =>
        toDouble(rawValue) <= ruleValueToDouble(rule.value)
      case "is_between" =>
        rule.value match {
          case Some(ListValue(lo :: hi :: Nil)) =>
            val v = toDouble(rawValue)
            v >= ruleValueToDouble(Some(lo)) && v <= ruleValueToDouble(Some(hi))
          case _ => false
        }

      case "is_equal_than" =>
        val other = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        Option(rawValue).map(_.toString).getOrElse("") ==
          Option(values.getOrElse(other, null)).map(_.toString).getOrElse("")

      // Membership
      case "is_contained_in" | "is_in" =>
        val vals = listValuesAsString(rule.value)
        vals.contains(rawValue.toString)
      case "not_contained_in" | "not_in" =>
        val vals = listValuesAsString(rule.value)
        !vals.contains(rawValue.toString)

      // Pattern
      case "has_pattern" =>
        val pattern = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        rawValue.toString.matches(pattern)
      case "is_legit" =>
        rawValue != null && rawValue.toString.trim.nonEmpty

      // Date
      case "all_date_checks"               => rawValue != null && safeToDate(rawValue) != null
      case "is_today"                      => toDate(rawValue) == LocalDate.now()
      case "is_t_minus_1" | "is_yesterday" => toDate(rawValue) == LocalDate.now().minusDays(1)
      case "is_t_minus_2"                  => toDate(rawValue) == LocalDate.now().minusDays(2)
      case "is_t_minus_3"                  => toDate(rawValue) == LocalDate.now().minusDays(3)
      case "is_past_date"                  => toDate(rawValue).isBefore(LocalDate.now())
      case "is_future_date"                => toDate(rawValue).isAfter(LocalDate.now())
      case "is_on_weekday" =>
        val dow = toDate(rawValue).getDayOfWeek.getValue
        dow >= 1 && dow <= 5
      case "is_on_weekend" =>
        val dow = toDate(rawValue).getDayOfWeek.getValue
        dow == 6 || dow == 7
      case "is_on_monday"    => toDate(rawValue).getDayOfWeek.getValue == 1
      case "is_on_tuesday"   => toDate(rawValue).getDayOfWeek.getValue == 2
      case "is_on_wednesday" => toDate(rawValue).getDayOfWeek.getValue == 3
      case "is_on_thursday"  => toDate(rawValue).getDayOfWeek.getValue == 4
      case "is_on_friday"    => toDate(rawValue).getDayOfWeek.getValue == 5
      case "is_on_saturday"  => toDate(rawValue).getDayOfWeek.getValue == 6
      case "is_on_sunday"    => toDate(rawValue).getDayOfWeek.getValue == 7
      case "is_date_between" =>
        rule.value match {
          case Some(ListValue(StringValue(s) :: StringValue(e) :: Nil)) =>
            val d = toDate(rawValue)
            !d.isBefore(LocalDate.parse(s)) && !d.isAfter(LocalDate.parse(e))
          case _ => false
        }
      case "is_date_after" =>
        val target = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        toDate(rawValue).isAfter(LocalDate.parse(target))
      case "is_date_before" =>
        val target = rule.value.collect { case StringValue(s) => s }.getOrElse("")
        toDate(rawValue).isBefore(LocalDate.parse(target))

      case "validate_date_format" =>
        if (rawValue == null) true
        else {
          val format = rule.value.collect { case StringValue(s) => s }.getOrElse("")
          try {
            LocalDate.parse(rawValue.toString, DateTimeFormatter.ofPattern(format))
            true
          } catch {
            case _: Exception => false
          }
        }

      case other =>
        throw new IllegalArgumentException(s"'$other' not implemented for the Flink streaming engine")
    }
  }

  /** Short "checkType:field" message used in the errors field. */
  private def buildErrorMessage(rule: RuleDefinition): String =
    s"${rule.checkType}:${rule.fieldName}"

  /** Converts a raw value to Double, throwing on incompatible types. */
  private def toDouble(v: Any): Double = v match {
    case n: Number => n.doubleValue()
    case s: String => s.toDouble
    case _         => throw new IllegalArgumentException(s"Cannot convert $v to Double")
  }

  /** Converts a raw value to LocalDate, throwing on incompatible types. */
  private def toDate(v: Any): LocalDate = v match {
    case d: LocalDate     => d
    case d: java.sql.Date => d.toLocalDate
    case s: String        => LocalDate.parse(s)
    case _                => throw new IllegalArgumentException(s"Cannot convert $v to LocalDate")
  }

  /** Converts a raw value to LocalDate, returning null instead of throwing. */
  private def safeToDate(v: Any): LocalDate =
    try toDate(v)
    catch {
      case _: Exception => null
    }

  /** Converts a numeric RuleValue to Double, throwing on non-numeric values. */
  private def ruleValueToDouble(v: Option[io.galileostd.sumeh.rule.RuleValue]): Double = v match {
    case Some(LongValue(l))   => l.toDouble
    case Some(DoubleValue(d)) => d
    case Some(StringValue(s)) => s.toDouble
    case _                    => throw new IllegalArgumentException(s"Expected numeric RuleValue, got $v")
  }

  /** Extracts the string forms of a ListValue for membership checks. */
  private def listValuesAsString(v: Option[io.galileostd.sumeh.rule.RuleValue]): Set[String] = v match {
    case Some(ListValue(items)) =>
      items.map {
        case StringValue(s) => s
        case LongValue(l)   => l.toString
        case DoubleValue(d) => d.toString
        case other          => other.toString
      }.toSet
    case _ => Set.empty
  }
}
