package io.galileostd.sumeh.flink.internal

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.util.Locale

import io.galileostd.sumeh.rule.{
  DoubleValue,
  ListValue,
  LongValue,
  RuleDefinition,
  RuleRegistry,
  RuleValue,
  StringValue
}
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
   * Pre-compiled regexes for every `has_pattern` rule, keyed by the regex itself.
   *
   * Compiled eagerly once per operator (not per record), so an invalid regex fails at job construction. Keyed by the
   * regex string rather than `checkType` so two `has_pattern` rules with different patterns each use their own.
   */
  private val patternCache: Map[String, java.util.regex.Pattern] =
    rules
      .filter(
        r =>
          RuleRegistry.canonical(r.checkType) == "has_pattern" &&
            r.isApplicableForLevel("ROW") &&
            r.skipReason("ROW", "flink-streaming").isEmpty
      )
      .map {
        r =>
          val regex = DQProcessFunction.requireString(r, "has_pattern requires a regex pattern")
          regex -> java.util.regex.Pattern.compile(regex)
      }
      .toMap

  /**
   * Pre-compiled formatters for every `validate_date_format` rule, keyed by the format string.
   *
   * Compiled eagerly once per operator (not per record) with an explicit `Locale.ROOT`, so patterns with text fields
   * (e.g. `MMM`, `EEE`) parse identically on every TaskManager instead of following each JVM's default locale.
   */
  private val dateFormatCache: Map[String, DateTimeFormatter] =
    rules
      .filter(
        r =>
          RuleRegistry.canonical(r.checkType) == "validate_date_format" &&
            r.isApplicableForLevel("ROW") &&
            r.skipReason("ROW", "flink-streaming").isEmpty
      )
      .map {
        r =>
          val format = DQProcessFunction.requireString(r, "validate_date_format requires a format string as value")
          format -> DateTimeFormatter.ofPattern(format, Locale.ROOT)
      }
      .toMap

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

    val (errors, skipped) = DQProcessFunction.evaluate(values, rules, patternCache, dateFormatCache)

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
    expected: Option[String] = None,
    actual: Option[String] = None,
    message: Option[String] = None,
    timestamp: String
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
   * Args: values: Field-name-to-value map of the record. rules: Rules to evaluate. patterns: Pre-compiled `has_pattern`
   * regexes keyed by `checkType` (compiled once per operator, not per record). formats: Pre-compiled
   * `validate_date_format` formatters keyed by format string (compiled once per operator, not per record).
   *
   * Returns: A tuple of structured error entries and skipped-rule reasons.
   */
  private[flink] def evaluate(
      values: Map[String, Any],
      rules: Seq[RuleDefinition],
      patterns: Map[String, java.util.regex.Pattern] = Map.empty,
      formats: Map[String, DateTimeFormatter] = Map.empty
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
              if (!checkRule(values, rule, patterns, formats)) errors += buildError(rule)
            catch {
              case e: Exception => errors += buildError(rule, Some(s"ERROR[${rule.checkType}]: ${e.getMessage}"))
            }
        }
      }

    (errors.toList, skipped.toList)
  }

  /**
   * Extracts and validates everything that depends on `value`, once, at job construction.
   *
   * Throws on the first malformed rule, so a configuration error surfaces before the job is submitted instead of one
   * error per record at runtime. Rules that will be skipped anyway (execute=false, wrong level, unsupported engine) are
   * not validated — there is no point failing the job over a rule that never runs.
   *
   * Args: rules: The rules to validate.
   *
   * Throws: IllegalArgumentException on the first rule with a missing or wrong-typed `value`.
   */
  private[flink] def validateRules(rules: Seq[RuleDefinition]): Unit =
    rules.foreach {
      rule =>
        if (rule.isApplicableForLevel("ROW") && rule.skipReason("ROW", "flink-streaming").isEmpty)
          RuleRegistry.canonical(rule.checkType) match {
            case "has_pattern" =>
              java.util.regex.Pattern.compile(requireString(rule, "has_pattern requires a regex pattern"))
            case "is_between" =>
              requirePair(rule, "is_between requires value=[min, max]")
            case "is_date_between" =>
              requirePair(rule, "is_date_between requires value=[start, end]")
            case "is_contained_in" | "not_contained_in" =>
              requireList(rule, s"${rule.checkType} requires a list of values")
            case "is_equal_than" =>
              requireString(rule, "is_equal_than requires a column name as value")
            case "is_date_after" =>
              requireString(rule, "is_date_after requires a date value")
            case "is_date_before" =>
              requireString(rule, "is_date_before requires a date value")
            case "validate_date_format" =>
              requireString(rule, "validate_date_format requires a format string as value")
            case _ => ()
          }
    }

  /**
   * Validates that every rule field exists in the stream, once, at job construction.
   *
   * A rule targeting a missing column would otherwise read null for every record and silently pass the non-completeness
   * null short-circuit, contradicting the "no silent passes" contract. The field names are known from the stream's
   * `RowTypeInfo`, so a typo is caught before the job is submitted. Rules that will be skipped anyway are not checked.
   *
   * Args: rules: The rules to validate. fieldNames: Input field names, in positional order.
   *
   * Throws: IllegalArgumentException on the first rule whose field is absent from `fieldNames`.
   */
  private[flink] def validateFields(rules: Seq[RuleDefinition], fieldNames: Array[String]): Unit =
    rules.foreach {
      rule =>
        if (rule.isApplicableForLevel("ROW") && rule.skipReason("ROW", "flink-streaming").isEmpty)
          rule.field.fold(f => List(f), identity).foreach {
            f =>
              if (!fieldNames.contains(f))
                throw new IllegalArgumentException(
                  s"Rule '${rule.checkType}' targets field '$f', which is not in the stream (fields: ${fieldNames.mkString(", ")})"
                )
          }
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
      "expected"   -> e.expected.map(ujson.Str(_)).getOrElse(ujson.Null),
      "actual"     -> e.actual.map(ujson.Str(_)).getOrElse(ujson.Null),
      "message"    -> e.message.map(ujson.Str(_)).getOrElse(ujson.Null),
      "timestamp"  -> ujson.Str(e.timestamp)
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
  private def checkRule(
      values: Map[String, Any],
      rule: RuleDefinition,
      patterns: Map[String, java.util.regex.Pattern] = Map.empty,
      formats: Map[String, DateTimeFormatter] = Map.empty
  ): Boolean = {
    val field     = rule.field.fold(identity, _.head)
    val rawValue  = values.getOrElse(field, null)
    val checkType = RuleRegistry.canonical(rule.checkType)

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
        val (lo, hi) = requirePair(rule, "is_between requires value=[min, max]")
        val v        = toDouble(rawValue)
        v >= ruleValueToDouble(Some(lo)) && v <= ruleValueToDouble(Some(hi))

      case "is_equal_than" =>
        val other = requireString(rule, "is_equal_than requires a column name as value")
        Option(rawValue).map(_.toString).getOrElse("") ==
          Option(values.getOrElse(other, null)).map(_.toString).getOrElse("")

      // Membership
      case "is_contained_in" =>
        val vals = listValuesAsString(rule)
        vals.contains(rawValue.toString)
      case "not_contained_in" =>
        val vals = listValuesAsString(rule)
        !vals.contains(rawValue.toString)

      // Pattern
      case "has_pattern" =>
        val regex   = requireString(rule, "has_pattern requires a regex pattern")
        val pattern = patterns.getOrElse(regex, java.util.regex.Pattern.compile(regex))
        pattern.matcher(rawValue.toString).find()
      case "is_legit" =>
        rawValue != null && rawValue.toString.trim.nonEmpty

      // Date
      case "all_date_checks" => rawValue != null && safeToDate(rawValue) != null
      case "is_today"        => toDate(rawValue) == LocalDate.now()
      case "is_t_minus_1"    => toDate(rawValue) == LocalDate.now().minusDays(1)
      case "is_t_minus_2"    => toDate(rawValue) == LocalDate.now().minusDays(2)
      case "is_t_minus_3"    => toDate(rawValue) == LocalDate.now().minusDays(3)
      case "is_past_date"    => toDate(rawValue).isBefore(LocalDate.now())
      case "is_future_date"  => toDate(rawValue).isAfter(LocalDate.now())
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
        val (start, end) = requirePair(rule, "is_date_between requires value=[start, end]") match {
          case (StringValue(s), StringValue(e)) => (s, e)
          case _ =>
            throw new IllegalArgumentException("is_date_between requires [start, end] date strings")
        }
        val d = toDate(rawValue)
        !d.isBefore(LocalDate.parse(start)) && !d.isAfter(LocalDate.parse(end))
      case "is_date_after" =>
        val target = requireString(rule, "is_date_after requires a date value")
        toDate(rawValue).isAfter(LocalDate.parse(target))
      case "is_date_before" =>
        val target = requireString(rule, "is_date_before requires a date value")
        toDate(rawValue).isBefore(LocalDate.parse(target))

      case "validate_date_format" =>
        if (rawValue == null) true
        else {
          val format    = requireString(rule, "validate_date_format requires a format string as value")
          val formatter = formats.getOrElse(format, DateTimeFormatter.ofPattern(format, Locale.ROOT))
          try {
            LocalDate.parse(rawValue.toString, formatter)
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
   * Extracts the string value of a rule, throwing when absent.
   *
   * Args: rule: The rule. msg: The error message when the value is missing.
   *
   * Returns: The `StringValue` contents.
   *
   * Throws: IllegalArgumentException when `value` is missing or not a string.
   */
  private[flink] def requireString(rule: RuleDefinition, msg: String): String =
    rule.value
      .collect { case StringValue(s) => s }
      .getOrElse(throw new IllegalArgumentException(msg))

  /**
   * Extracts the list value of a rule, throwing when absent.
   *
   * Args: rule: The rule. msg: The error message when the value is missing.
   *
   * Returns: The `ListValue` items.
   *
   * Throws: IllegalArgumentException when `value` is missing or not a list.
   */
  private[flink] def requireList(rule: RuleDefinition, msg: String): List[RuleValue] =
    rule.value
      .collect { case ListValue(items) => items }
      .getOrElse(throw new IllegalArgumentException(msg))

  /**
   * Extracts a `(lo, hi)` pair from the rule's list value, throwing when absent.
   *
   * Args: rule: The rule. msg: The error message when the value is absent.
   *
   * Returns: The two items of the `ListValue`.
   *
   * Throws: IllegalArgumentException when `value` is missing or is not a two-element list.
   */
  private[flink] def requirePair(rule: RuleDefinition, msg: String): (RuleValue, RuleValue) =
    rule.value match {
      case Some(ListValue(lo :: hi :: Nil)) => (lo, hi)
      case _                                => throw new IllegalArgumentException(msg)
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
      expected = rule.value.map(_.toString),
      timestamp = java.time.LocalDateTime.now(java.time.ZoneOffset.UTC).toString
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
   * Args: rule: The rule holding the membership list.
   *
   * Returns: A Set of the string forms of every item.
   *
   * Throws: IllegalArgumentException when `value` is missing or not a list.
   */
  private def listValuesAsString(rule: RuleDefinition): Set[String] =
    requireList(rule, s"${rule.checkType} requires a list of values").map {
      case StringValue(s) => s
      case LongValue(l)   => l.toString
      case DoubleValue(d) => d.toString
      case other          => other.toString
    }.toSet
}
