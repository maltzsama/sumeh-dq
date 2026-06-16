package io.galileostd.sumeh.flink

import java.time.{ LocalDate, LocalDateTime }
import java.util.UUID
import scala.jdk.CollectionConverters._

import io.galileostd.sumeh.rule.{
  BoolValue,
  DateTimeValue,
  DateValue,
  DoubleValue,
  ListValue,
  LongValue,
  RuleDefinition,
  RuleRegistry,
  RuleValue,
  StringValue
}
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationResult, ValidationStatus }
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.{ DataStream, SingleOutputStreamOperator }
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{ Collector, OutputTag }

// ============================================================================
// ValidatedFlinkStream
// ============================================================================

/**
 * Wrapper around a Flink DataStream[Row] with _dq_errors field. Provides split() via SideOutput — zero reprocessing,
 * zero shuffle.
 */
class ValidatedFlinkStream(
    private val stream: SingleOutputStreamOperator[Row],
    private val errorTag: OutputTag[Row],
    val goodTag: OutputTag[Row]
) {
  def split(): (DataStream[Row], DataStream[Row]) = {
    val good = stream.getSideOutput(goodTag)
    val bad  = stream.getSideOutput(errorTag)
    (good, bad)
  }

  def toNative: SingleOutputStreamOperator[Row] = stream
}

// ============================================================================
// FlinkValidator
// ============================================================================

object FlinkValidator {

  /**
   * Validate a Flink DataStream[Row] using the Bifurcation Pattern.
   *
   * Adds _dq_errors to each Row via MapFunction (stateless, scalable). Uses SideOutput for split — no reprocessing.
   *
   * Example: val result = FlinkValidator.validate(stream, rules) val (good, bad) = result.split()
   * good.sinkTo(kafkaCleanSink) bad.sinkTo(kafkaDlqSink)
   */
  def validate(
      stream: DataStream[Row],
      rules: Seq[RuleDefinition]
  ): ValidatedFlinkStream = {

    val rowRules = rules.filter(_.isApplicableForLevel("ROW"))

    val gTag = new OutputTag[Row]("_dq_good") {}
    val eTag = new OutputTag[Row]("_dq_errors") {}

    val processed = stream.process(new DQProcessFunction(rowRules, gTag, eTag))

    new ValidatedFlinkStream(processed, eTag, gTag) // passa os dois
  }
}

// ============================================================================
// DQProcessFunction — row-level validation + SideOutput routing
// ============================================================================

private class DQProcessFunction(
    rules: Seq[RuleDefinition],
    goodTag: OutputTag[Row],
    errorTag: OutputTag[Row]
) extends ProcessFunction[Row, Row] {

  override def processElement(
      row: Row,
      ctx: ProcessFunction[Row, Row]#Context,
      out: Collector[Row]
  ): Unit = {

    val errors = scala.collection.mutable.ListBuffer[String]()

    for (rule <- rules)
      rule.skipReason("ROW", "flink") match {
        case Some(_) => // skip silently
        case None =>
          try {
            val passed = checkRule(row, rule)
            if (!passed) {
              errors += buildErrorMessage(rule)
            }
          } catch {
            case e: Exception =>
              errors += s"ERROR[${rule.checkType}]: ${e.getMessage}"
          }
      }

    // Build enriched row with _dq_errors field
    val fieldCount = row.getArity
    val enriched   = Row.withNames()

    // Copy all original fields
    (0 until fieldCount).foreach {
      i =>
        val name = row.getFieldNames(true).toArray()(i).asInstanceOf[String]
        enriched.setField(name, row.getField(i))
    }

    // Add _dq_errors as comma-separated string (Flink Row doesn't support arrays natively)
    enriched.setField("_dq_errors", errors.mkString("|"))

    if (errors.isEmpty) {
      ctx.output(goodTag, enriched)
    } else {
      ctx.output(errorTag, enriched)
    }

    // Main output gets everything (for chaining)
    out.collect(enriched)
  }

  // -------------------------------------------------------------------------
  // Rule evaluation — pure row-level, no aggregation
  // -------------------------------------------------------------------------

  private def checkRule(row: Row, rule: RuleDefinition): Boolean = {
    val field     = rule.field.fold(identity, _.head)
    val rawValue  = row.getField(field)
    val checkType = rule.checkType

    if (rawValue == null && checkType != "is_complete" && checkType != "is_legit")
      return true // null values skip non-completeness checks (consistent with Spark)

    checkType match {
      // Completeness
      case "is_complete" | "are_complete" =>
        val fields = rule.field.fold(List(_), identity)
        fields.forall(f => row.getField(f) != null)

      // Uniqueness — not meaningful in stateless streaming
      case "is_unique" | "are_unique" | "is_primary_key" | "is_composite_key" =>
        true // skip: uniqueness requires state, use Flink StatefulFunction separately

      // Comparison
      case "is_positive"    => toDouble(rawValue) > 0
      case "is_negative"    => toDouble(rawValue) < 0
      case "is_in_millions" => toDouble(rawValue) >= 1_000_000L
      case "is_in_billions" => toDouble(rawValue) >= 1_000_000_000L

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
        rawValue.toString == row.getField(other).toString

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

      case _ => true // unknown check type: pass through
    }
  }

  private def buildErrorMessage(rule: RuleDefinition): String =
    s"${rule.checkType}:${rule.fieldName}"

  private def toDouble(v: Any): Double = v match {
    case n: Number => n.doubleValue()
    case s: String => s.toDouble
    case _         => throw new IllegalArgumentException(s"Cannot convert $v to Double")
  }

  private def toDate(v: Any): LocalDate = v match {
    case d: LocalDate     => d
    case d: java.sql.Date => d.toLocalDate
    case s: String        => LocalDate.parse(s)
    case _                => throw new IllegalArgumentException(s"Cannot convert $v to LocalDate")
  }

  private def ruleValueToDouble(v: Option[RuleValue]): Double = v match {
    case Some(LongValue(l))   => l.toDouble
    case Some(DoubleValue(d)) => d
    case Some(StringValue(s)) => s.toDouble
    case _                    => throw new IllegalArgumentException(s"Expected numeric RuleValue, got $v")
  }

  private def listValuesAsString(v: Option[RuleValue]): Set[String] = v match {
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
