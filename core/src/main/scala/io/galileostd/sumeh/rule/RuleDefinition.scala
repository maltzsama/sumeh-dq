package io.galileostd.sumeh.rule

import java.time.{ LocalDate, LocalDateTime }
import scala.util.Try

import io.galileostd.sumeh.exception.SumehException

/**
 * Data quality rule with validation and metadata preservation.
 *
 * @param field
 *   Column name(s) to validate
 * @param checkType
 *   Validation rule type (must exist in RuleRegistry)
 * @param value
 *   Threshold or comparison value
 * @param threshold
 *   Pass rate threshold (0.0–1.0)
 * @param execute
 *   Whether rule should be executed
 * @param level
 *   Validation level (auto-populated from registry)
 * @param category
 *   Rule category (auto-populated from registry)
 * @param updatedAt
 *   Rule update timestamp
 * @param metadata
 *   Extra fields from source (preserved)
 */
final case class RuleDefinition(
    field: Either[String, List[String]],
    checkType: String,
    value: Option[RuleValue] = None,
    threshold: Double = 1.0,
    execute: Boolean = true,
    level: String = "ROW",
    category: String = "unknown",
    updatedAt: Option[LocalDateTime] = None,
    metadata: Map[String, Any] = Map.empty
) {
  def fieldName: String = field.fold(identity, _.mkString(","))

  def isApplicableForLevel(targetLevel: String): Boolean = {
    val normalized = level.toUpperCase.replace("_LEVEL", "")
    val target     = targetLevel.toUpperCase.replace("_LEVEL", "")
    normalized == target
  }

  def skipReason(targetLevel: String, engine: String): Option[String] =
    if (!execute) Some("execute=false")
    else if (!isApplicableForLevel(targetLevel))
      Some(s"Wrong level: expected '$targetLevel', got '$level'")
    else if (!RuleRegistry.isSupported(checkType, engine))
      Some(s"Engine '$engine' not supported for rule '$checkType'")
    else None

  override def toString: String = {
    val f    = field.fold(identity, cols => s"[${cols.mkString(",")}]")
    val meta = if (metadata.nonEmpty) s", +${metadata.size} meta" else ""
    s"RuleDef(field=$f, check=$checkType, level=$level, category=$category$meta)"
  }
}

object RuleDefinition {

  /**
   * Smart constructor — validates against RuleRegistry and enriches level/category from manifest, same as Python's
   * __post_init__.
   */
  def validated( // ← era apply
      field: Either[String, List[String]],
      checkType: String,
      value: Option[RuleValue] = None,
      threshold: Double = 1.0,
      execute: Boolean = true,
      updatedAt: Option[LocalDateTime] = None,
      metadata: Map[String, Any] = Map.empty
  ): RuleDefinition = {
    val entry = RuleRegistry
      .getRule(checkType)
      .getOrElse(
        throw new SumehException(
          s"Invalid rule type '$checkType'. " +
            s"Available: ${RuleRegistry.listRules().take(10).mkString(", ")}..."
        )
      )

    new RuleDefinition(
      field = field,
      checkType = checkType,
      value = value,
      threshold = threshold,
      execute = execute,
      level = entry.level,
      category = entry.category,
      updatedAt = updatedAt,
      metadata = metadata
    )
  }

  /** Creates RuleDefinition from a raw Map, preserving unknown keys as metadata. */
  def fromMap(data: Map[String, Any]): RuleDefinition = {
    val knownFields = Set(
      "field",
      "check_type",
      "value",
      "threshold",
      "level",
      "category",
      "execute",
      "updated_at"
    )

    val field = parseField(data.getOrElse("field", ""))

    val value = data.get("value").flatMap(parseValue)

    val threshold = data.get("threshold").flatMap(v => Try(v.toString.toDouble).toOption).getOrElse(1.0)

    val execute = data
      .get("execute")
      .map {
        case b: Boolean => b
        case s: String  => Set("true", "1", "yes", "y", "t").contains(s.toLowerCase)
        case _          => true
      }
      .getOrElse(true)

    val updatedAt = data.get("updated_at").flatMap(parseTimestamp)

    val metadata = data.view.filterKeys(k => !knownFields.contains(k)).toMap

    val checkType = data
      .get("check_type")
      .map(_.toString)
      .getOrElse(throw new SumehException("Missing required field: check_type"))

    RuleDefinition.validated(
      field = field,
      checkType = checkType,
      value = value,
      threshold = threshold,
      execute = execute,
      updatedAt = updatedAt,
      metadata = metadata
    )
  }

  def parseField(input: Any): Either[String, List[String]] = input match {
    case list: List[_] =>
      val cols = list.map(_.toString.trim)
      if (cols.size > 1) Right(cols) else Left(cols.headOption.getOrElse(""))

    case s: String =>
      val trimmed = s.trim
        .stripPrefix("\"")
        .stripSuffix("\"")
        .stripPrefix("'")
        .stripSuffix("'")
        .trim

      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        val inner = trimmed.drop(1).dropRight(1).trim
        val cols =
          inner.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"").stripPrefix("'").stripSuffix("'")).toList
        if (cols.size > 1) Right(cols) else Left(cols.headOption.getOrElse(""))
      } else if (trimmed.contains(",")) {
        val cols = trimmed.split(",").map(_.trim).toList
        if (cols.size > 1) Right(cols) else Left(cols.headOption.getOrElse(""))
      } else {
        Left(trimmed)
      }

    case other => Left(other.toString.trim)
  }

  def parseValue(input: Any): Option[RuleValue] = input match {
    case null                                              => None
    case s: String if s.toUpperCase == "NULL" || s.isEmpty => None
    case b: Boolean                                        => Some(BoolValue(b))
    case i: Int                                            => Some(LongValue(i.toLong))
    case l: Long                                           => Some(LongValue(l))
    case f: Float                                          => Some(DoubleValue(f.toDouble))
    case d: Double                                         => Some(DoubleValue(d))
    case d: LocalDate                                      => Some(DateValue(d))
    case dt: LocalDateTime                                 => Some(DateTimeValue(dt))
    case list: List[_]                                     => Some(ListValue(list.flatMap(v => parseValue(v))))
    case s: String =>
      val trimmed = s.trim
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        val inner = trimmed.drop(1).dropRight(1)
        val items = inner.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).toList
        Some(ListValue(items.flatMap(v => parseValue(v))))
      } else {
        Try(LocalDate.parse(trimmed))
          .map(d => Some(DateValue(d)))
          .getOrElse(
            Try(trimmed.toLong)
              .map(l => Some(LongValue(l)))
              .getOrElse(
                Try(trimmed.toDouble)
                  .map(d => Some(DoubleValue(d)))
                  .getOrElse(
                    Some(StringValue(trimmed))
                  )
              )
          )
      }
    case other => Some(StringValue(other.toString))
  }

  private def parseTimestamp(input: Any): Option[LocalDateTime] = input match {
    case dt: LocalDateTime => Some(dt)
    case s: String         => Try(LocalDateTime.parse(s)).toOption
    case _                 => None
  }
}

/** ADT for rule values — replaces Python's Any-typed value field. */
sealed trait RuleValue
final case class StringValue(v: String)          extends RuleValue
final case class LongValue(v: Long)              extends RuleValue
final case class DoubleValue(v: Double)          extends RuleValue
final case class BoolValue(v: Boolean)           extends RuleValue
final case class DateValue(v: LocalDate)         extends RuleValue
final case class DateTimeValue(v: LocalDateTime) extends RuleValue
final case class ListValue(v: List[RuleValue])   extends RuleValue
