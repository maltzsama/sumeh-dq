package io.galileostd.sumeh.rule

import java.time.LocalDate

import io.galileostd.sumeh.exception.SumehException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RuleDefinitionSpec extends AnyWordSpec with Matchers {

  "RuleDefinition.validated" should {

    "create a valid rule" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.field shouldBe Left("email")
      rule.checkType shouldBe "is_complete"
      rule.threshold shouldBe 1.0
      rule.execute shouldBe true
    }

    "throw SumehException for invalid check_type" in {
      an[SumehException] should be thrownBy {
        RuleDefinition.validated(Left("email"), "fake_check_xyz")
      }
    }

    "auto-enrich level as ROW for row rules" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.level shouldBe "ROW"
    }

    "auto-enrich level as TABLE for table rules" in {
      val rule = RuleDefinition.validated(Left("age"), "has_mean")
      rule.level shouldBe "TABLE"
    }

    "auto-enrich category" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.category shouldBe "completeness"
    }

    "default metadata to empty map" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.metadata shouldBe empty
    }

    "default execute to true" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.execute shouldBe true
    }
  }

  "RuleDefinition.parseField" should {

    "parse simple string" in {
      RuleDefinition.parseField("email") shouldBe Left("email")
    }

    "parse list of strings" in {
      RuleDefinition.parseField(List("name", "email")) shouldBe Right(List("name", "email"))
    }

    "parse bracket notation" in {
      RuleDefinition.parseField("[name, email]") shouldBe Right(List("name", "email"))
    }

    "unwrap single item list" in {
      RuleDefinition.parseField(List("email")) shouldBe Left("email")
    }

    "strip quotes from field name" in {
      RuleDefinition.parseField("\"email\"") shouldBe Left("email")
    }

    "parse comma-separated string" in {
      RuleDefinition.parseField("name, email") shouldBe Right(List("name", "email"))
    }
  }

  "RuleDefinition.parseValue" should {

    "return None for null" in {
      RuleDefinition.parseValue(null) shouldBe None
    }

    "return None for NULL string" in {
      RuleDefinition.parseValue("NULL") shouldBe None
    }

    "return None for empty string" in {
      RuleDefinition.parseValue("") shouldBe None
    }

    "parse boolean" in {
      RuleDefinition.parseValue(true) shouldBe Some(BoolValue(true))
    }

    "parse int as LongValue" in {
      RuleDefinition.parseValue(42) shouldBe Some(LongValue(42L))
    }

    "parse long" in {
      RuleDefinition.parseValue(42L) shouldBe Some(LongValue(42L))
    }

    "parse double" in {
      RuleDefinition.parseValue(40.0) shouldBe Some(DoubleValue(40.0))
    }

    "parse date string" in {
      RuleDefinition.parseValue("2020-01-01") shouldBe Some(DateValue(LocalDate.of(2020, 1, 1)))
    }

    "parse numeric string as LongValue" in {
      RuleDefinition.parseValue("42") shouldBe Some(LongValue(42L))
    }

    "parse float string as DoubleValue" in {
      RuleDefinition.parseValue("40.5") shouldBe Some(DoubleValue(40.5))
    }

    "keep non-numeric string as StringValue" in {
      RuleDefinition.parseValue("^[a-z]+$") shouldBe Some(StringValue("^[a-z]+$"))
    }

    "parse list notation as ListValue" in {
      val result = RuleDefinition.parseValue("[18, 120]")
      result shouldBe defined
      result.get shouldBe a[ListValue]
    }
  }

  "RuleDefinition.fromMap" should {

    "create rule from map" in {
      val rule = RuleDefinition.fromMap(
        Map(
          "field"      -> "email",
          "check_type" -> "is_complete",
          "threshold"  -> "1.0"
        )
      )
      rule.field shouldBe Left("email")
      rule.checkType shouldBe "is_complete"
      rule.threshold shouldBe 1.0
    }

    "preserve extra fields as metadata" in {
      val rule = RuleDefinition.fromMap(
        Map(
          "field"       -> "email",
          "check_type"  -> "is_complete",
          "environment" -> "prod",
          "table_name"  -> "users"
        )
      )
      rule.metadata("environment") shouldBe "prod"
      rule.metadata("table_name") shouldBe "users"
    }

    "parse execute=true string" in {
      val rule = RuleDefinition.fromMap(
        Map(
          "field"      -> "email",
          "check_type" -> "is_complete",
          "execute"    -> "true"
        )
      )
      rule.execute shouldBe true
    }

    "parse execute=false string" in {
      val rule = RuleDefinition.fromMap(
        Map(
          "field"      -> "email",
          "check_type" -> "is_complete",
          "execute"    -> "false"
        )
      )
      rule.execute shouldBe false
    }

    "default threshold to 1.0 on invalid value" in {
      val rule = RuleDefinition.fromMap(
        Map(
          "field"      -> "email",
          "check_type" -> "is_complete",
          "threshold"  -> "not_a_number"
        )
      )
      rule.threshold shouldBe 1.0
    }

    "throw SumehException when check_type missing" in {
      an[SumehException] should be thrownBy {
        RuleDefinition.fromMap(Map("field" -> "email"))
      }
    }
  }

  "RuleDefinition" should {

    "check isApplicableForLevel for ROW" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.isApplicableForLevel("ROW") shouldBe true
      rule.isApplicableForLevel("TABLE") shouldBe false
    }

    "check isApplicableForLevel for TABLE" in {
      val rule = RuleDefinition.validated(Left("age"), "has_mean")
      rule.isApplicableForLevel("TABLE") shouldBe true
      rule.isApplicableForLevel("ROW") shouldBe false
    }

    "return skipReason for execute=false" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete", execute = false)
      rule.skipReason("ROW", "spark") shouldBe defined
      rule.skipReason("ROW", "spark").get should include("execute")
    }

    "return skipReason for wrong level" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.skipReason("TABLE", "spark") shouldBe defined
    }

    "return skipReason for unsupported engine" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.skipReason("ROW", "oracle") shouldBe defined
      rule.skipReason("ROW", "oracle").get should include("oracle")
    }

    "return None skipReason when applicable" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.skipReason("ROW", "spark") shouldBe None
    }

    "have meaningful toString" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      val str  = rule.toString
      str should include("email")
      str should include("is_complete")
    }
  }

  "RuleLoader round-trip" should {

    import io.galileostd.sumeh.config.RuleLoader

    "round-trip values through CSV losslessly" in {
      val rule = RuleDefinition.validated(
        Left("status"),
        "is_contained_in",
        value = Some(ListValue(List(StringValue("active"), StringValue("inactive"), StringValue("pending")))),
        threshold = 0.9
      )
      val back = RuleLoader.fromCsvString(RuleLoader.toCsv(List(rule)))
      back should have size 1
      back.head.value shouldBe rule.value
      back.head.threshold shouldBe rule.threshold
      back.head.checkType shouldBe rule.checkType
    }

    "round-trip values through JSON losslessly" in {
      val rules = List(
        RuleDefinition.validated(Left("age"), "is_greater_than", value = Some(LongValue(18))),
        RuleDefinition.validated(Left("avg"), "has_mean", value = Some(DoubleValue(29.4))),
        RuleDefinition.validated(Left("dt"), "is_equal", value = Some(DateValue(LocalDate.of(2020, 1, 1)))),
        RuleDefinition.validated(Left("flag"), "is_equal", value = Some(BoolValue(true))),
        RuleDefinition.validated(
          Left("status"),
          "is_contained_in",
          value = Some(ListValue(List(StringValue("active"), StringValue("inactive"))))
        )
      )
      val back = RuleLoader.fromJsonString(RuleLoader.toJson(rules))
      back should have size rules.size
      back.map(_.value) shouldBe rules.map(_.value)
    }

    "parse a single JSON object (not wrapped in an array)" in {
      val json = """{"field": "email", "check_type": "is_complete"}"""
      val back = RuleLoader.fromJsonString(json)
      back should have size 1
      back.head.checkType shouldBe "is_complete"
    }
  }

  "RuleValue.toTaggedString / parseValue round-trip" should {

    "round-trip every value type" in {
      val values = List[RuleValue](
        StringValue("abc"),
        LongValue(42L),
        DoubleValue(3.14),
        BoolValue(true),
        BoolValue(false),
        DateValue(LocalDate.of(2020, 1, 1)),
        DateTimeValue(java.time.LocalDateTime.of(2020, 1, 1, 10, 30)),
        ListValue(List(StringValue("a"), LongValue(1)))
      )
      values.foreach(v => RuleDefinition.parseValue(v.toTaggedString) shouldBe Some(v))
    }
  }

  "RuleValue.toAny" should {

    "convert scalars to plain JVM values" in {
      RuleValue.toAny(StringValue("s")) shouldBe "s"
      RuleValue.toAny(LongValue(1L)) shouldBe 1L
      RuleValue.toAny(DoubleValue(1.5)) shouldBe 1.5
      RuleValue.toAny(BoolValue(true)) shouldBe true
    }

    "convert dates to java.sql.Date" in {
      RuleValue.toAny(DateValue(LocalDate.of(2020, 1, 1))) shouldBe java.sql.Date.valueOf("2020-01-01")
    }

    "convert timestamps to java.sql.Timestamp" in {
      val dt = java.time.LocalDateTime.of(2020, 1, 1, 10, 0)
      RuleValue.toAny(DateTimeValue(dt)) shouldBe java.sql.Timestamp.valueOf(dt)
    }

    "convert lists element-wise" in {
      RuleValue.toAny(ListValue(List(LongValue(1), LongValue(2)))) shouldBe List(1L, 2L)
    }
  }

  "RuleDefinition.parseValue" should {

    "parse tagged value strings" in {
      RuleDefinition.parseValue("LongValue(5)") shouldBe Some(LongValue(5))
      RuleDefinition.parseValue("BoolValue(true)") shouldBe Some(BoolValue(true))
      RuleDefinition.parseValue("DateValue(2020-01-01)") shouldBe Some(DateValue(LocalDate.of(2020, 1, 1)))
    }

    "parse literal true/false strings as booleans" in {
      RuleDefinition.parseValue("true") shouldBe Some(BoolValue(true))
      RuleDefinition.parseValue("false") shouldBe Some(BoolValue(false))
    }

    "parse a list of numbers" in {
      RuleDefinition.parseValue("[18, 120]") shouldBe Some(ListValue(List(LongValue(18), LongValue(120))))
    }

    "parse a quoted list of strings" in {
      RuleDefinition.parseValue("[\"active\",\"inactive\"]") shouldBe
      Some(ListValue(List(StringValue("active"), StringValue("inactive"))))
    }
  }

  "RuleDefinition.parseField" should {

    "normalize a quoted string" in {
      RuleDefinition.parseField("  'email'  ") shouldBe Left("email")
    }

    "parse an empty string as empty field" in {
      RuleDefinition.parseField("") shouldBe Left("")
    }

    "fall back to toString for other types" in {
      RuleDefinition.parseField(42) shouldBe Left("42")
    }
  }

  "RuleDefinition.fromMap" should {

    "parse a multi-field bracket notation" in {
      val rule = RuleDefinition.fromMap(Map("field" -> "[id, name]", "check_type" -> "are_complete"))
      rule.field shouldBe Right(List("id", "name"))
    }

    "parse updated_at" in {
      val rule = RuleDefinition.fromMap(
        Map(
          "field"      -> "email",
          "check_type" -> "is_complete",
          "updated_at" -> "2024-01-01T10:00:00"
        )
      )
      rule.updatedAt shouldBe defined
    }

    "ignore updated_at when unparseable" in {
      val rule = RuleDefinition.fromMap(Map("field" -> "email", "check_type" -> "is_complete", "updated_at" -> "nope"))
      rule.updatedAt shouldBe None
    }

    "parse execute from 0/1 strings" in {
      RuleDefinition
        .fromMap(Map("field" -> "a", "check_type" -> "is_complete", "execute" -> "0"))
        .execute shouldBe false
      RuleDefinition.fromMap(Map("field" -> "a", "check_type" -> "is_complete", "execute" -> "1")).execute shouldBe true
    }
  }

  "RuleDefinition.isApplicableForLevel" should {

    "ignore _LEVEL suffixes" in {
      val rule = RuleDefinition.validated(Left("email"), "is_complete")
      rule.isApplicableForLevel("row_level") shouldBe true
      rule.isApplicableForLevel("table_level") shouldBe false
    }
  }
}
