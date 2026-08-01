package io.galileostd.sumeh.flink.internal

import java.time.format.DateTimeFormatter
import java.time.LocalDate

import io.galileostd.sumeh.rule.{ DoubleValue, ListValue, LongValue, RuleDefinition, StringValue }
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{ Collector, OutputTag }

/**
 * Flink `ProcessFunction` that evaluates each record against the rules.
 *
 * Enriches every record with `_dq_errors` and `_dq_skipped` fields, emits the enriched row once on the main output, and
 * routes records with at least one error to the error side output. Field names come from the input `RowTypeInfo`, so
 * positional rows are supported. Evaluation is stateless — one record at a time.
 *
 * Args: rules: Rules to evaluate. fieldNames: Input field names, in positional order (from the stream's `RowTypeInfo`).
 * errorTag: Side-output tag for records with at least one error.
 */
private[flink] class DQProcessFunction(
    rules: Seq[RuleDefinition],
    fieldNames: Array[String],
    errorTag: OutputTag[Row]
) extends ProcessFunction[Row, Row] {

  /**
   * Evaluates one record and emits the enriched row.
   *
   * The record's fields are copied by position into a widened row and augmented with `_dq_errors` (a JSON array of
   * error objects) and `_dq_skipped` (pipe-separated reasons). The enriched row is always emitted once on the main
   * output; records with at least one error are additionally routed to `errorTag`.
   *
   * Args: row: The input record. ctx: The process context used to write side outputs. out: The main output collector.
   */
  override def processElement(
      row: Row,
      ctx: ProcessFunction[Row, Row]#Context,
      out: Collector[Row]
  ): Unit = {

    val values = fieldNames.indices.map(i => fieldNames(i) -> row.getField(i)).toMap

    val (errors, skipped) = DQProcessFunction.evaluate(values, rules)

    val enriched = new Row(fieldNames.length + 2)
    var i        = 0
    while (i < fieldNames.length) {
      enriched.setField(i, row.getField(i))
      i += 1
    }
    enriched.setField(fieldNames.length, DQProcessFunction.errorsToJson(errors))
    enriched.setField(fieldNames.length + 1, skipped.mkString("|"))

    out.collect(enriched)
    if (errors.nonEmpty) ctx.output(errorTag, enriched)
  }
}

/**
 * One structured error entry, mirroring the Spark `_dq_errors` struct fields so a consumer can treat both engines
 * uniformly via `from_json`.
 */
final private[flink] case class DQError(
    rule_id: String,
    check_type: String,
    field: String,
    category: String,
    message: Option[String] = None,
    expected: Option[String] = None,
    actual: Option[String] = None
)

/**
 * Companion with the pure, cluster-free rule evaluation logic.
 *
 * Kept free of Flink runtime types so the rule logic can be unit tested without a cluster.
 */
private[flink] object DQProcessFunction {

  /**
   * Pure row-level evaluation of a single record.
   *
   * Returns a `(errors, skippedReasons)` pair. TABLE-level rules are always skipped with an explanatory reason; rules
   * whose `skipReason` yields a reason are skipped; any exception thrown while evaluating a rule is captured as an
   * ERROR entry.
   *
   * Args: values: Field-name-to-value map of the record. rules: Rules to evaluate.
   *
   * Returns: A tuple of structured error entries and skipped-rule reasons.
   */
  private[flink] def evaluate(
      values: Map[String, Any],
      rules: Seq[RuleDefinition]
  ): (List[DQError], List[String]) = {
    val errors  = scala.collection.mutable.ListBuffer[DQError]()
    val skipped = scala.collection.mutable.ListBuffer[String]()

    for (rule <- rules)
      if (!rule.isApplicableForLevel("ROW")) {
        skipped += s"${rule.checkType}:TABLE-level rules are not supported in streaming"
      } else {
        rule.skipReason("ROW", "flink-streaming") match {
          case Some(reason) => skipped += s"${rule.checkType}:$reason"
          case None =>
            try
              if (!checkRule(values, rule)) errors += buildError(rule)
            catch {
              case e: Exception => errors += buildError(rule, Some(s"ERROR[${rule.checkType}]: ${e.getMessage}"))
            }
        }
      }

    (errors.toList, skipped.toList)
  }

  /**
   * Serializes error entries as a JSON array string, matching the Spark `_dq_errors` struct fields.
   *
   * Args: errors: The error entries for a record.
   *
   * Returns: A JSON array string (e.g. `[{"check_type":"is_complete",...}]`), or `[]` when empty.
   */
  private[flink] def errorsToJson(errors: List[DQError]): String =
    ujson.write(ujson.Arr.from(errors.map(errorToJson)))

  private def errorToJson(e: DQError): ujson.Obj =
    ujson.Obj(
      "rule_id"    -> ujson.Str(e.rule_id),
      "check_type" -> ujson.Str(e.check_type),
      "field"      -> ujson.Str(e.field),
      "category"   -> ujson.Str(e.category),
      "message"    -> e.message.map(ujson.Str(_)).getOrElse(ujson.Null),
      "expected"   -> e.expected.map(ujson.Str(_)).getOrElse(ujson.Null),
      "actual"     -> e.actual.map(ujson.Str(_)).getOrElse(ujson.Null)
    )

  // -------------------------------------------------------------------------
  // Rule evaluation — pure row-level, no aggregation
  // -------------------------------------------------------------------------

  /**
   * Checks one row against a single rule — pure row-level logic, no aggregation.
   *
   * Null values short-circuit non-completeness checks as passing (consistent with Spark), except for completeness,
   * `is_legit` and `validate_date_format`. Unsupported check types throw.
   *
   * Args: values: Field-name-to-value map of the record. rule: The rule to evaluate.
   *
   * Returns: True if the record satisfies the rule.
   *
   * Throws: IllegalArgumentException if the check type is not implemented for the Flink streaming engine, or if a value
   * cannot be converted to the type the check requires.
   */
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

  /**
   * Builds a structured error entry for a failed rule.
   *
   * Args: rule: The failed rule. message: Optional message (a short `checkType:field` by default, or a full ERROR
   * message when an exception was caught).
   *
   * Returns: A [[DQError]] mirroring the Spark `_dq_errors` struct.
   */
  private def buildError(rule: RuleDefinition, message: Option[String] = None): DQError =
    DQError(
      rule_id = java.util.UUID.randomUUID().toString,
      check_type = rule.checkType,
      field = rule.fieldName,
      category = rule.category,
      message = message.orElse(Some(s"${rule.checkType}:${rule.fieldName}")),
      expected = rule.value.map(_.toString)
    )

  /**
   * Converts a raw value to Double, throwing on incompatible types.
   *
   * Args: v: The raw value.
   *
   * Returns: The value as a Double.
   *
   * Throws: IllegalArgumentException if `v` is neither a Number nor a String parseable as a Double.
   */
  private def toDouble(v: Any): Double = v match {
    case n: Number => n.doubleValue()
    case s: String => s.toDouble
    case _         => throw new IllegalArgumentException(s"Cannot convert $v to Double")
  }

  /**
   * Converts a raw value to LocalDate, throwing on incompatible types.
   *
   * Args: v: The raw value.
   *
   * Returns: The value as a LocalDate.
   *
   * Throws: IllegalArgumentException if `v` is neither a LocalDate, a java.sql.Date, nor a String parseable as an ISO
   * date.
   */
  private def toDate(v: Any): LocalDate = v match {
    case d: LocalDate     => d
    case d: java.sql.Date => d.toLocalDate
    case s: String        => LocalDate.parse(s)
    case _                => throw new IllegalArgumentException(s"Cannot convert $v to LocalDate")
  }

  /**
   * Converts a raw value to LocalDate, returning null instead of throwing.
   *
   * Args: v: The raw value.
   *
   * Returns: The value as a LocalDate, or null if it cannot be parsed.
   */
  private def safeToDate(v: Any): LocalDate =
    try toDate(v)
    catch {
      case _: Exception => null
    }

  /**
   * Converts a numeric RuleValue to Double, throwing on non-numeric values.
   *
   * Args: v: The optional RuleValue (Long, Double or numeric String).
   *
   * Returns: The value as a Double.
   *
   * Throws: IllegalArgumentException if `v` is empty or not numeric.
   */
  private def ruleValueToDouble(v: Option[io.galileostd.sumeh.rule.RuleValue]): Double = v match {
    case Some(LongValue(l))   => l.toDouble
    case Some(DoubleValue(d)) => d
    case Some(StringValue(s)) => s.toDouble
    case _                    => throw new IllegalArgumentException(s"Expected numeric RuleValue, got $v")
  }

  /**
   * Extracts the string forms of a ListValue for membership checks.
   *
   * Args: v: The optional RuleValue holding the membership list.
   *
   * Returns: A Set of the string forms of every item, or an empty Set when `v` is not a ListValue.
   */
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
