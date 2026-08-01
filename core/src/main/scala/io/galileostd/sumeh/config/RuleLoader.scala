package io.galileostd.sumeh.config

import scala.util.Try

import io.galileostd.sumeh.rule.RuleDefinition

/**
 * Load and export RuleDefinition lists from/to strings.
 *
 * PURE PARSING — no I/O, no disk, no S3, no GCS, no Azure, no DBFS.
 *
 * User is responsible for reading from wherever they want:
 *   - S3, GCS, Azure Blob, DBFS, HDFS, JDBC, HTTP, local file, etc
 *   - Then pass the string to fromCsvString() or fromJsonString()
 *
 * Export: toCsv() and toJson() return strings that user can save anywhere.
 */
object RuleLoader {

  // -------------------------------------------------------------------------
  // LOAD from strings
  // -------------------------------------------------------------------------

  /** Parse rules from a CSV string (`field,check_type,value,threshold,execute,level,category`). */
  def fromCsvString(csv: String): List[RuleDefinition] = {
    val lines = csv.linesIterator.toList
    if (lines.isEmpty) return List.empty

    val header = parseCsvLine(lines.head)
    lines.tail
      .filter(_.trim.nonEmpty)
      .map {
        line =>
          val values = parseCsvLine(line)
          val row    = header.zipAll(values, "", "").toMap
          RuleDefinition.fromMap(row.filter(_._2.nonEmpty))
      }
  }

  /** Parse rules from a JSON string — a single rule object or an array of rule objects. */
  def fromJsonString(json: String): List[RuleDefinition] = {
    import upickle.default._

    if (json == null || json.trim.isEmpty) return List.empty

    val parsed = Try(ujson.read(json)).getOrElse(ujson.Null)

    val rows: List[ujson.Obj] = parsed match {
      case arr: ujson.Arr => arr.value.toList.collect { case o: ujson.Obj => o }
      case obj: ujson.Obj => List(obj)
      case _              => List.empty
    }

    rows.map {
      row =>
        val strMap = row.value.map { case (k, v) => k -> jsonValueToString(v) }.toMap
        RuleDefinition.fromMap(strMap)
    }
  }

  // -------------------------------------------------------------------------
  // EXPORT to strings
  // -------------------------------------------------------------------------

  /** Serialize rules to a CSV string, using the lossless tagged [[RuleValue]] format for `value`. */
  def toCsv(rules: List[RuleDefinition]): String = {
    val header = "field,check_type,value,threshold,execute,level,category"
    val lines = rules.map {
      r =>
        val value     = r.value.map(_.toTaggedString).getOrElse("")
        val threshold = r.threshold.toString
        val execute   = r.execute.toString
        val level     = r.level
        val category  = r.category
        List(
          quoteCsv(r.fieldName),
          quoteCsv(r.checkType),
          quoteCsv(value),
          quoteCsv(threshold),
          quoteCsv(execute),
          quoteCsv(level),
          quoteCsv(category)
        ).mkString(",")
    }
    (header +: lines).mkString("\n")
  }

  /** Serialize rules to a JSON array string (metadata and `value` are stringified). */
  def toJson(rules: List[RuleDefinition]): String = {
    val arr = rules.map {
      r =>
        val base = Map(
          "field"      -> ujson.Str(r.fieldName),
          "check_type" -> ujson.Str(r.checkType),
          "threshold"  -> ujson.Num(r.threshold),
          "execute"    -> ujson.Bool(r.execute),
          "level"      -> ujson.Str(r.level),
          "category"   -> ujson.Str(r.category)
        ) ++
          r.value.map(v => "value" -> ruleValueToJson(v)).toMap ++
          r.updatedAt.map(dt => "updated_at" -> ujson.Str(dt.toString)).toMap

        // Metadata: converte Any pra string
        val metadataJson = r.metadata.map {
          case (k, v) =>
            k -> ujson.Str(v.toString)
        }

        ujson.Obj.from(base ++ metadataJson)
    }

    ujson.write(ujson.Arr.from(arr))
  }

  // -------------------------------------------------------------------------
  // CSV parsing (internal)
  // -------------------------------------------------------------------------

  /** Parse one CSV line, honoring quoted fields and `""` escapes. */
  private def parseCsvLine(line: String): List[String] = {
    val result  = scala.collection.mutable.ListBuffer[String]()
    val current = new StringBuilder
    var inQuote = false
    var i       = 0

    while (i < line.length) {
      val ch = line.charAt(i)

      if (inQuote) {
        if (ch == '"') {
          if (i + 1 < line.length && line.charAt(i + 1) == '"') {
            current.append('"')
            i += 2
          } else {
            inQuote = false
            i += 1
          }
        } else {
          current.append(ch)
          i += 1
        }
      } else {
        ch match {
          case '"' =>
            inQuote = true
            i += 1
          case ',' =>
            result += current.toString.trim
            current.clear()
            i += 1
          case _ =>
            current.append(ch)
            i += 1
        }
      }
    }

    result += current.toString.trim
    result.toList
  }

  /** Quote a CSV field only when it contains a comma, quote, or newline. */
  private def quoteCsv(field: String): String = {
    val needsQuoting = field.contains(",") || field.contains("\"") || field.contains("\n")
    if (needsQuoting) {
      val escaped = field.replace("\"", "\"\"")
      s""""$escaped""""
    } else field
  }

  // -------------------------------------------------------------------------
  // JSON helpers (internal)
  // -------------------------------------------------------------------------

  /** Flatten a ujson value to a plain string for [[RuleDefinition.fromMap]]. */
  private def jsonValueToString(v: ujson.Value): String = v match {
    case ujson.Str(s)   => s
    case ujson.Num(n)   => if (n == n.toLong) n.toLong.toString else n.toString
    case ujson.Bool(b)  => b.toString
    case ujson.Null     => ""
    case ujson.Arr(arr) => arr.map(jsonValueToString).mkString("[", ",", "]")
    case ujson.Obj(obj) => obj.map { case (k, v) => s"$k:${jsonValueToString(v)}" }.mkString("{", ",", "}")
  }

  /** Convert a [[RuleValue]] to its ujson representation. */
  private def ruleValueToJson(v: io.galileostd.sumeh.rule.RuleValue): ujson.Value = v match {
    case io.galileostd.sumeh.rule.StringValue(s)    => ujson.Str(s)
    case io.galileostd.sumeh.rule.LongValue(l)      => ujson.Num(l.toDouble)
    case io.galileostd.sumeh.rule.DoubleValue(d)    => ujson.Num(d)
    case io.galileostd.sumeh.rule.BoolValue(b)      => ujson.Bool(b)
    case io.galileostd.sumeh.rule.DateValue(d)      => ujson.Str(d.toString)
    case io.galileostd.sumeh.rule.DateTimeValue(dt) => ujson.Str(dt.toString)
    case io.galileostd.sumeh.rule.ListValue(items)  => ujson.Arr.from(items.map(ruleValueToJson))
  }
}
