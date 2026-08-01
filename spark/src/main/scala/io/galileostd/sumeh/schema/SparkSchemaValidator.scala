package io.galileostd.sumeh.spark.schema

import io.galileostd.sumeh.schema.{ ColumnDef, SchemaDef, SchemaReport }
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

/**
 * Schema validator for Spark DataFrames. Mirrors Python's validate_schema / extract_schema for the Spark engine.
 */
object SparkSchemaValidator {

  // -------------------------------------------------------------------------
  // Type mapping — Spark DataType → canonical type
  // -------------------------------------------------------------------------

  private val typeMap: Map[String, String] = Map(
    "byte"          -> "integer",
    "tinyint"       -> "integer",
    "short"         -> "integer",
    "smallint"      -> "integer",
    "integer"       -> "integer",
    "int"           -> "integer",
    "long"          -> "integer",
    "bigint"        -> "integer",
    "float"         -> "float",
    "double"        -> "float",
    "decimal"       -> "float",
    "string"        -> "string",
    "varchar"       -> "string",
    "char"          -> "string",
    "binary"        -> "binary",
    "boolean"       -> "boolean",
    "date"          -> "datetime",
    "timestamp"     -> "datetime",
    "timestamp_ntz" -> "datetime",
    "array"         -> "array",
    "struct"        -> "complex",
    "map"           -> "complex"
  )

  private def toCanonical(dt: DataType): String = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType => "integer"
    case _: FloatType | _: DoubleType | _: DecimalType             => "float"
    case _: StringType                                             => "string"
    case _: BinaryType                                             => "binary"
    case _: BooleanType                                            => "boolean"
    case _: DateType | _: TimestampType | _: TimestampNTZType      => "datetime"
    case _: ArrayType                                              => "array"
    case _: StructType | _: MapType                                => "complex"
    case _                                                         => "unknown"
  }

  // -------------------------------------------------------------------------
  // Extract schema from DataFrame
  // -------------------------------------------------------------------------

  /** Extract actual schema as Map[colName → info]. */
  def extractSchema(df: DataFrame): Map[String, Map[String, Any]] =
    df.schema.fields.map {
      field =>
        val info = scala.collection.mutable.Map[String, Any](
          "raw_type" -> field.dataType.typeName,
          "nullable" -> field.nullable,
          "comment"  -> (if (field.metadata.contains("comment")) field.metadata.getString("comment") else "")
        )

        field.dataType match {
          case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
            info("nested_fields") = extractSchemaFromStructType(at.elementType.asInstanceOf[StructType])
          case at: ArrayType =>
            info("element_type") = toCanonical(at.elementType)
          case st: StructType =>
            info("nested_fields") = extractSchemaFromStructType(st)
          case _ =>
        }

        field.name -> info.toMap
    }.toMap

  private def extractSchemaFromStructType(st: StructType): Map[String, Map[String, Any]] =
    st.fields.map {
      f =>
        f.name -> Map[String, Any](
          "raw_type" -> f.dataType.typeName,
          "nullable" -> f.nullable,
          "comment"  -> ""
        )
    }.toMap

  // -------------------------------------------------------------------------
  // Validate schema
  // -------------------------------------------------------------------------

  /**
   * Validate a Spark DataFrame against a SchemaDef. Mirrors Python's validate() in schema/validator.py.
   */
  def validate(df: DataFrame, expected: SchemaDef): SchemaReport = {
    val actual = extractSchema(df)
    val report = validateRecursive(expected.columns, actual)

    if (expected.strictColumns) {
      val expectedNames = expected.columns.map(_.name).toSet
      val extra         = actual.keys.filterNot(expectedNames.contains).toList
      if (extra.nonEmpty)
        return report.copy(passed = false, extraCols = extra)
    }

    report
  }

  private def validateRecursive(
      expectedCols: List[ColumnDef],
      actualCols: Map[String, Map[String, Any]],
      parentPath: String = ""
  ): SchemaReport = {

    val missingCols    = scala.collection.mutable.ListBuffer[String]()
    val typeErrors     = scala.collection.mutable.Map[String, String]()
    val metadataErrors = scala.collection.mutable.Map[String, String]()

    for (colDef <- expectedCols) {
      val fullPath = if (parentPath.isEmpty) colDef.name else s"$parentPath.${colDef.name}"
      actualCols.get(colDef.name) match {

        case None =>
          if (!colDef.isOptional) missingCols += fullPath

        case Some(actual) =>
          // Type check
          val canonExpected = canonExpectedType(colDef.expectedType)
          val canonActual = toCanonical(
            // best-effort: map raw_type string back to DataType for canonical check
            rawToDataType(actual.getOrElse("raw_type", "").toString)
          )

          if (canonExpected != "unknown" && canonExpected != canonActual) {
            typeErrors(fullPath) = s"Expected $canonExpected, got $canonActual (${actual.getOrElse("raw_type", "?")})"
          } else {
            // Element type (array)
            for {
              expectedElem <- colDef.elementType
              actualElem   <- actual.get("element_type").map(_.toString)
            }
              if (expectedElem.toLowerCase != actualElem.toLowerCase)
                typeErrors(fullPath) = s"Array element type mismatch: expected $expectedElem, got $actualElem"

            // Comment validation
            val actualComment = actual.getOrElse("comment", "").toString.trim
            if (colDef.requireComment && actualComment.isEmpty)
              metadataErrors(fullPath) = "Missing required column comment"
            colDef.expectedComment.foreach {
              expected =>
                if (expected != actualComment)
                  metadataErrors(fullPath) = s"Comment mismatch. Expected: '$expected'"
            }

            // Nullability
            val actualNullable = actual.getOrElse("nullable", true).asInstanceOf[Boolean]
            if (!colDef.nullable && actualNullable)
              typeErrors(fullPath) = "Schema violation: contract requires NOT NULL but column allows nulls"

            // Nested fields
            for {
              nestedDefs   <- colDef.fields
              nestedActual <- actual.get("nested_fields").map(_.asInstanceOf[Map[String, Map[String, Any]]])
            } {
              val childReport = validateRecursive(nestedDefs, nestedActual, fullPath)
              missingCols ++= childReport.missingCols
              typeErrors ++= childReport.typeErrors
              metadataErrors ++= childReport.metadataErrors
            }
          }
      }
    }

    val passed = missingCols.isEmpty && typeErrors.isEmpty && metadataErrors.isEmpty
    SchemaReport(
      passed = passed,
      missingCols = missingCols.toList,
      typeErrors = typeErrors.toMap,
      metadataErrors = metadataErrors.toMap
    )
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Normalize an expected type so it compares against the canonical actual type (struct/map → complex). */
  private def canonExpectedType(t: String): String = {
    val low = t.toLowerCase
    if (low == "struct" || low == "map") "complex" else low
  }

  private def rawToDataType(raw: String): DataType = raw.toLowerCase.trim match {
    case "byte" | "tinyint"            => ByteType
    case "short" | "smallint"          => ShortType
    case "integer" | "int"             => IntegerType
    case "long" | "bigint"             => LongType
    case "float"                       => FloatType
    case "double"                      => DoubleType
    case s if s.startsWith("decimal")  => DecimalType.SYSTEM_DEFAULT
    case "string" | "varchar" | "char" => StringType
    case "binary"                      => BinaryType
    case "boolean"                     => BooleanType
    case "date"                        => DateType
    case "timestamp" | "timestamp_ntz" => TimestampType
    case s if s.startsWith("array")    => ArrayType(StringType)
    case s if s.startsWith("struct")   => new StructType()
    case s if s.startsWith("map")      => MapType(StringType, StringType)
    case _                             => StringType
  }
}
