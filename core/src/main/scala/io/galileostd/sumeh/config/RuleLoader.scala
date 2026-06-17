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

  def fromJsonString(json: String): List[RuleDefinition] = {
    import upickle.default.*

    Try(read[List[Map[String, ujson.Value]]](json))
      .getOrElse(List.empty)
      .map {
        row =>
          val strMap = row.map {
            case (k, v) =>
              k -> jsonValueToString(v)
          }
          RuleDefinition.fromMap(strMap)
      }
  }

  // -------------------------------------------------------------------------
  // EXPORT to strings
  // -------------------------------------------------------------------------

  def toCsv(rules: List[RuleDefinition]): String = {
    val header = "field,check_type,value,threshold,execute,level,category"
    val lines = rules.map {
      r =>
        val value     = r.value.map(_.toString).getOrElse("")
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
          r.value.map(v => "value" -> ujson.Str(v.toString)).toMap ++
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

  private def jsonValueToString(v: ujson.Value): String = v match {
    case ujson.Str(s)   => s
    case ujson.Num(n)   => if (n == n.toLong) n.toLong.toString else n.toString
    case ujson.Bool(b)  => b.toString
    case ujson.Null     => ""
    case ujson.Arr(arr) => arr.map(jsonValueToString).mkString("[", ",", "]")
    case ujson.Obj(obj) => obj.map { case (k, v) => s"$k:${jsonValueToString(v)}" }.mkString("{", ",", "}")
  }
}
