package io.galileostd.sumeh.schema

/**
 * Definition of a single column in a schema contract.
 *
 * @param name
 *   Column name
 * @param expectedType
 *   Expected canonical type ("string", "integer", "float", etc)
 * @param isOptional
 *   If true, missing column is not an error
 * @param nullable
 *   Whether the column allows nulls
 * @param elementType
 *   For array columns: element type (e.g. "string", "integer")
 * @param requireComment
 *   Whether a comment/description is required
 * @param expectedComment
 *   Expected comment text
 * @param fields
 *   Nested columns for struct types
 */
final case class ColumnDef(
    name: String,
    expectedType: String,
    isOptional: Boolean = false,
    nullable: Boolean = true,
    elementType: Option[String] = None,
    requireComment: Boolean = false,
    expectedComment: Option[String] = None,
    fields: Option[List[ColumnDef]] = None
)

object ColumnDef {

  /** Create from a Map — mirrors Python's from_dict. */
  def fromMap(name: String, props: Any): ColumnDef = props match {
    case s: String => ColumnDef(name = name, expectedType = s)
    case m: Map[_, _] =>
      val map = m.asInstanceOf[Map[String, Any]]
      val nested = map.get("fields").map {
        case f: Map[_, _] =>
          f.asInstanceOf[Map[String, Any]].map { case (k, v) => fromMap(k.toString, v) }.toList
        case _ => List.empty
      }
      ColumnDef(
        name = name,
        expectedType = map.getOrElse("type", "string").toString,
        isOptional = asBool(map.getOrElse("is_optional", false)),
        nullable = asBool(map.getOrElse("nullable", true)),
        elementType = map.get("element_type").map(_.toString),
        requireComment = asBool(map.getOrElse("require_comment", false)),
        expectedComment = map.get("expected_comment").map(_.toString),
        fields = nested
      )
    case _ => ColumnDef(name = name, expectedType = "string")
  }

  private def asBool(v: Any): Boolean = v match {
    case b: Boolean => b
    case s: String  => Set("true", "1", "yes", "y", "t").contains(s.trim.toLowerCase)
    case n: Number  => n.doubleValue() != 0
    case null       => false
    case _          => false
  }
}

/**
 * Schema definition — a list of column contracts.
 *
 * @param columns
 *   Column definitions
 * @param strictColumns
 *   If true, extra columns in the DataFrame are errors
 */
final case class SchemaDef(
    columns: List[ColumnDef],
    strictColumns: Boolean = false
)

object SchemaDef {
  def fromMap(data: Map[String, Any], strict: Boolean = false): SchemaDef =
    SchemaDef(
      columns = data.map { case (k, v) => ColumnDef.fromMap(k, v) }.toList,
      strictColumns = strict
    )
}

/**
 * Schema validation report.
 *
 * @param passed
 *   Whether validation passed
 * @param missingCols
 *   Columns required but not found
 * @param typeErrors
 *   Columns with wrong type: colName → error message
 * @param metadataErrors
 *   Columns with comment/nullability issues
 * @param extraCols
 *   Columns present but not in schema (only when strictColumns=true)
 */
final case class SchemaReport(
    passed: Boolean,
    missingCols: List[String] = List.empty,
    typeErrors: Map[String, String] = Map.empty,
    metadataErrors: Map[String, String] = Map.empty,
    extraCols: List[String] = List.empty
) {
  def totalIssues: Int =
    missingCols.size + typeErrors.size + metadataErrors.size + extraCols.size

  def toMap: Map[String, Any] = Map(
    "passed"          -> passed,
    "missing_columns" -> missingCols,
    "type_errors"     -> typeErrors,
    "metadata_errors" -> metadataErrors,
    "extra_columns"   -> extraCols,
    "total_issues"    -> totalIssues
  )

  override def toString: String = {
    val status = if (passed) "✓ PASSED" else "✗ FAILED"
    s"SchemaReport($status, $totalIssues issues)"
  }
}
