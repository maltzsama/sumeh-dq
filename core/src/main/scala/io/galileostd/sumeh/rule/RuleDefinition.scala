package io.galileostd.sumeh.rule

import java.time.{ LocalDate, LocalDateTime }
import scala.util.Try

import io.galileostd.sumeh.exception.SumehException

/**
 * Data quality rule with validation and metadata preservation.
 *
 * Args: field: Column name(s) to validate checkType: Validation rule type (must exist in RuleRegistry) value: Threshold
 * or comparison value threshold: Pass rate threshold (0.0–1.0) execute: Whether the rule should be executed level:
 * Validation level (auto-populated from registry) category: Rule category (auto-populated from registry) updatedAt:
 * Rule update timestamp metadata: Extra fields from source (preserved)
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

  /** Flattened column name(s): single name or comma-joined list. */
  def fieldName: String = field.fold(identity, _.mkString(","))

  /** Whether this rule applies at the given level (normalizes `ROW`/`ROW_LEVEL` style suffixes). */
  def isApplicableForLevel(targetLevel: String): Boolean = {
    val normalized = level.toUpperCase.replace("_LEVEL", "")
    val target     = targetLevel.toUpperCase.replace("_LEVEL", "")
    normalized == target
  }

  /** Reason this rule would be skipped at `targetLevel` on `engine`, or `None` if it can run. */
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

/** Companion with smart constructors and value/field parsing helpers. */
object RuleDefinition {

  /**
   * Smart constructor — validates against RuleRegistry and enriches level/category from manifest, same as Python's
   * __post_init__. Throws [[io.galileostd.sumeh.exception.SumehException]] on an unknown `checkType`.
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
        case s: String =>
          val t = s.trim.toLowerCase
          t.isEmpty || Set("true", "1", "yes", "y", "t").contains(t)
        case _ => true
      }
      .getOrElse(true)

    val updatedAt = data.get("updated_at").flatMap(parseTimestamp)

    val metadata = data.filterNot { case (k, _) => knownFields.contains(k) }

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

  /** Parse a `field` value into a single-column `Left` or multi-column `Right`, supporting list/`[a,b]`/`a,b` forms. */
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

  /** Parse a raw `value` (from JSON/CSV/maps) into a [[RuleValue]], handling the tagged string forms used by CSV. */
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
    case s: String if s.startsWith("StringValue(") && s.endsWith(")") =>
      Some(StringValue(s.stripPrefix("StringValue(").stripSuffix(")")))
    case s: String if s.startsWith("LongValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("LongValue(").stripSuffix(")")).collect { case l: LongValue => l }
    case s: String if s.startsWith("DoubleValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DoubleValue(").stripSuffix(")")).collect { case d: DoubleValue => d }
    case s: String if s.startsWith("BoolValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("BoolValue(").stripSuffix(")")).collect { case b: BoolValue => b }
    case s: String if s.startsWith("DateValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DateValue(").stripSuffix(")")).collect { case d: DateValue => d }
    case s: String if s.startsWith("DateTimeValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DateTimeValue(").stripSuffix(")")).collect { case dt: DateTimeValue => dt }
    case s: String if s.startsWith("ListValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("ListValue(").stripSuffix(")"))
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
            Try(LocalDateTime.parse(trimmed))
              .map(dt => Some(DateTimeValue(dt)))
              .getOrElse(
                Try(trimmed.toLong)
                  .map(l => Some(LongValue(l)))
                  .getOrElse(
                    Try(trimmed.toDouble)
                      .map(d => Some(DoubleValue(d)))
                      .getOrElse(
                        if (trimmed == "true" || trimmed == "false") Some(BoolValue(trimmed.toBoolean))
                        else Some(StringValue(trimmed))
                      )
                  )
              )
          )
      }
    case other => Some(StringValue(other.toString))
  }

  /** Parses an `updated_at` value into a LocalDateTime. */
  private def parseTimestamp(input: Any): Option[LocalDateTime] = input match {
    case dt: LocalDateTime => Some(dt)
    case s: String         => Try(LocalDateTime.parse(s)).toOption
    case _                 => None
  }
}

/** ADT for rule values — replaces Python's Any-typed value field. */
sealed trait RuleValue {

  /** Lossless export form used by RuleLoader (round-trips through parseValue). */
  def toTaggedString: String
}

/** Companion with value conversion helpers. */
object RuleValue {

  /**
   * Converts a RuleValue to a plain JVM value (Spark F.lit-friendly: dates as java.sql.Date).
   *
   * Args: v: The rule value.
   *
   * Returns: A plain JVM value (String, Long, Double, Boolean, java.sql.Date/Timestamp, or List).
   */
  def toAny(v: RuleValue): Any = v match {
    case StringValue(s)    => s
    case LongValue(l)      => l
    case DoubleValue(d)    => d
    case BoolValue(b)      => b
    case DateValue(d)      => java.sql.Date.valueOf(d)
    case DateTimeValue(dt) => java.sql.Timestamp.valueOf(dt)
    case ListValue(items)  => items.map(toAny)
  }
}

/** String rule value. */
final case class StringValue(v: String) extends RuleValue {
  def toTaggedString: String = s"StringValue($v)"
}

/** Long rule value. */
final case class LongValue(v: Long) extends RuleValue {
  def toTaggedString: String = s"LongValue($v)"
}

/** Double rule value. */
final case class DoubleValue(v: Double) extends RuleValue {
  def toTaggedString: String = s"DoubleValue($v)"
}

/** Boolean rule value. */
final case class BoolValue(v: Boolean) extends RuleValue {
  def toTaggedString: String = s"BoolValue($v)"
}

/** Date rule value (local date, no time). */
final case class DateValue(v: LocalDate) extends RuleValue {
  def toTaggedString: String = s"DateValue($v)"
}

/** Date-time rule value. */
final case class DateTimeValue(v: LocalDateTime) extends RuleValue {
  def toTaggedString: String = s"DateTimeValue($v)"
}

/** List of rule values (used by is_between, is_contained_in, ...). */
final case class ListValue(v: List[RuleValue]) extends RuleValue {
  def toTaggedString: String = s"ListValue([${v.map(_.toTaggedString).mkString(",")}])"
}
