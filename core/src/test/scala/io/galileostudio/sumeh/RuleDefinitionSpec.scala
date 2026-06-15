package io.galileostudio.sumeh.rule

import io.galileostudio.sumeh.exception.SumehException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

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
      val rule = RuleDefinition.fromMap(Map(
        "field"      -> "email",
        "check_type" -> "is_complete",
        "threshold"  -> "1.0"
      ))
      rule.field shouldBe Left("email")
      rule.checkType shouldBe "is_complete"
      rule.threshold shouldBe 1.0
    }

    "preserve extra fields as metadata" in {
      val rule = RuleDefinition.fromMap(Map(
        "field"        -> "email",
        "check_type"   -> "is_complete",
        "environment"  -> "prod",
        "table_name"   -> "users"
      ))
      rule.metadata("environment") shouldBe "prod"
      rule.metadata("table_name") shouldBe "users"
    }

    "parse execute=true string" in {
      val rule = RuleDefinition.fromMap(Map(
        "field"      -> "email",
        "check_type" -> "is_complete",
        "execute"    -> "true"
      ))
      rule.execute shouldBe true
    }

    "parse execute=false string" in {
      val rule = RuleDefinition.fromMap(Map(
        "field"      -> "email",
        "check_type" -> "is_complete",
        "execute"    -> "false"
      ))
      rule.execute shouldBe false
    }

    "default threshold to 1.0 on invalid value" in {
      val rule = RuleDefinition.fromMap(Map(
        "field"      -> "email",
        "check_type" -> "is_complete",
        "threshold"  -> "not_a_number"
      ))
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
}