package io.galileostd.sumeh.schema

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SchemaModelsSpec extends AnyWordSpec with Matchers {

  "ColumnDef.fromMap" should {

    "create a column from a plain type string" in {
      val col = ColumnDef.fromMap("id", "integer")
      col.name shouldBe "id"
      col.expectedType shouldBe "integer"
      col.isOptional shouldBe false
      col.nullable shouldBe true
    }

    "read type from a map" in {
      val col = ColumnDef.fromMap("name", Map("type" -> "string"))
      col.expectedType shouldBe "string"
    }

    "default type to string" in {
      val col = ColumnDef.fromMap("x", Map.empty[String, Any])
      col.expectedType shouldBe "string"
    }

    "coerce boolean strings" in {
      ColumnDef.fromMap("a", Map("is_optional" -> "true")).isOptional shouldBe true
      ColumnDef.fromMap("b", Map("nullable" -> "false")).nullable shouldBe false
      ColumnDef.fromMap("c", Map("require_comment" -> "1")).requireComment shouldBe true
      ColumnDef.fromMap("d", Map("is_optional" -> "0")).isOptional shouldBe false
    }

    "coerce boolean values" in {
      ColumnDef.fromMap("a", Map("is_optional" -> true)).isOptional shouldBe true
      ColumnDef.fromMap("b", Map("nullable" -> false)).nullable shouldBe false
    }

    "read element type for arrays" in {
      val col = ColumnDef.fromMap("tags", Map("type" -> "array", "element_type" -> "string"))
      col.elementType shouldBe Some("string")
    }

    "read comment requirements" in {
      val col = ColumnDef.fromMap("name", Map("require_comment" -> true, "expected_comment" -> "full name"))
      col.requireComment shouldBe true
      col.expectedComment shouldBe Some("full name")
    }

    "parse nested fields" in {
      val col = ColumnDef.fromMap(
        "address",
        Map(
          "type"   -> "struct",
          "fields" -> Map("street" -> "string", "zip" -> Map("type" -> "integer"))
        )
      )
      col.fields shouldBe defined
      col.fields.get should have size 2
      col.fields.get.find(_.name == "zip").get.expectedType shouldBe "integer"
    }

    "parse nested fields given as a list of field objects" in {
      val col = ColumnDef.fromMap(
        "address",
        Map(
          "type" -> "struct",
          "fields" -> List(
            Map("name" -> "street", "type" -> "string"),
            Map("name" -> "zip", "type"    -> "integer")
          )
        )
      )
      col.fields shouldBe defined
      col.fields.get should have size 2
      col.fields.get.find(_.name == "zip").get.expectedType shouldBe "integer"
    }

    "yield no nested fields for an unrecognized fields shape" in {
      val col = ColumnDef.fromMap("address", Map("type" -> "struct", "fields" -> 42))
      col.fields shouldBe None
    }

    "throw a clear message for a list entry without a name" in {
      an[IllegalArgumentException] should be thrownBy ColumnDef.fromMap(
        "address",
        Map("type" -> "struct", "fields" -> List(Map("type" -> "string")))
      )
    }

    "default to string for unknown payloads" in {
      val col = ColumnDef.fromMap("x", 42)
      col.expectedType shouldBe "string"
    }
  }

  "SchemaDef.fromMap" should {

    "build a column per entry" in {
      val defn = SchemaDef.fromMap(Map("id" -> "integer", "name" -> "string"))
      defn.columns should have size 2
      defn.columns.find(_.name == "id").get.expectedType shouldBe "integer"
    }

    "carry the strict flag" in {
      SchemaDef.fromMap(Map.empty[String, Any], strict = true).strictColumns shouldBe true
      SchemaDef.fromMap(Map.empty[String, Any]).strictColumns shouldBe false
    }
  }

  "SchemaReport" should {

    "count total issues across all buckets" in {
      val report = SchemaReport(
        passed = false,
        missingCols = List("a"),
        typeErrors = Map("b" -> "wrong"),
        metadataErrors = Map("c" -> "missing comment"),
        extraCols = List("d")
      )
      report.totalIssues shouldBe 4
    }

    "expose every bucket in toMap" in {
      val report = SchemaReport(passed = false, missingCols = List("a"))
      val m      = report.toMap
      (m.keySet should contain).allOf(
        "passed",
        "missing_columns",
        "type_errors",
        "metadata_errors",
        "extra_columns",
        "total_issues"
      )
      m("missing_columns") shouldBe List("a")
      m("total_issues") shouldBe 1
    }

    "render PASS/FAIL in toString" in {
      SchemaReport(passed = true).toString should include("PASSED")
      SchemaReport(passed = false).toString should include("FAILED")
    }
  }

  "CR-31 strict coercions" should {
    "reject a structurally-wrong nullable in a schema map" in {
      an[IllegalArgumentException] should be thrownBy
      SchemaDef.fromMap(Map("id" -> Map("type" -> "integer", "nullable" -> List("true"))))
    }
  }
}
