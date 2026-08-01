package io.galileostd.sumeh.config

import io.galileostd.sumeh.exception.SumehException
import io.galileostd.sumeh.rule._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RuleLoaderSpec extends AnyWordSpec with Matchers {

  private val rule = RuleDefinition.validated(
    Left("email"),
    "is_complete",
    threshold = 0.9,
    metadata = Map("env" -> "prod")
  )

  "RuleLoader.fromCsvString" should {

    "parse a simple CSV into rules" in {
      val csv =
        """field,check_type,threshold
          |email,is_complete,1.0
          |age,is_positive,0.5
          |""".stripMargin
      val rules = RuleLoader.fromCsvString(csv)
      rules should have size 2
      rules.head.checkType shouldBe "is_complete"
      rules.head.field shouldBe Left("email")
      rules.head.threshold shouldBe 1.0
      rules(1).threshold shouldBe 0.5
    }

    "skip blank lines" in {
      val csv =
        """field,check_type
          |email,is_complete
          |
          |
          |age,is_positive
          |""".stripMargin
      RuleLoader.fromCsvString(csv) should have size 2
    }

    "return empty for an empty string" in {
      RuleLoader.fromCsvString("") shouldBe empty
    }

    "return empty for a header-only CSV" in {
      RuleLoader.fromCsvString("field,check_type") shouldBe empty
    }

    "parse a quoted field containing a comma as a multi-field rule" in {
      val csv =
        """field,check_type
          |"id,name",are_complete
          |""".stripMargin
      val rules = RuleLoader.fromCsvString(csv)
      rules.head.field shouldBe Right(List("id", "name"))
    }

    "unquote values and parse them" in {
      val csv =
        """field,check_type,value
          |status,is_contained_in,"active,inactive"
          |""".stripMargin
      val rules = RuleLoader.fromCsvString(csv)
      rules.head.value shouldBe defined
    }

    "preserve extra columns as metadata" in {
      val csv =
        """field,check_type,owner
          |email,is_complete,data-team
          |""".stripMargin
      val rules = RuleLoader.fromCsvString(csv)
      rules.head.metadata("owner") shouldBe "data-team"
    }

    "throw when check_type is missing" in {
      val csv = "field\nemail\n"
      an[SumehException] should be thrownBy RuleLoader.fromCsvString(csv)
    }

    "round-trip a tagged value through CSV" in {
      val r = RuleDefinition.validated(
        Left("status"),
        "is_contained_in",
        value = Some(ListValue(List(StringValue("a"), StringValue("b"))))
      )
      val back = RuleLoader.fromCsvString(RuleLoader.toCsv(List(r)))
      back.head.value shouldBe r.value
    }

    "round-trip tolerance through CSV" in {
      val r    = RuleDefinition.validated(Left("age"), "has_sum", value = Some(LongValue(100)), tolerance = 0.05)
      val back = RuleLoader.fromCsvString(RuleLoader.toCsv(List(r)))
      back.head.tolerance shouldBe 0.05
    }
  }

  "RuleLoader.toCsv" should {

    "emit the expected header" in {
      val csv = RuleLoader.toCsv(List(rule))
      csv.linesIterator.next() shouldBe "field,check_type,value,threshold,tolerance,execute,level,category"
    }

    "emit one line per rule" in {
      val csv = RuleLoader.toCsv(List(rule, rule))
      csv.linesIterator.size shouldBe 3 // header + 2
    }

    "quote fields that contain commas" in {
      val multi = RuleDefinition.validated(Right(List("id", "name")), "are_complete")
      val csv   = RuleLoader.toCsv(List(multi))
      csv.linesIterator.toList(1) should startWith("\"id,name\"")
    }

    "write values in tagged form" in {
      val r    = RuleDefinition.validated(Left("age"), "is_greater_than", value = Some(LongValue(18)))
      val csv  = RuleLoader.toCsv(List(r))
      val line = csv.linesIterator.toList(1)
      line should include("LongValue(18)")
    }
  }

  "RuleLoader.fromJsonString" should {

    "parse an array of rules" in {
      val json =
        """[
          |  {"field": "email", "check_type": "is_complete", "threshold": 0.9},
          |  {"field": "age", "check_type": "is_positive"}
          |]""".stripMargin
      val rules = RuleLoader.fromJsonString(json)
      rules should have size 2
      rules.head.threshold shouldBe 0.9
    }

    "parse a single object" in {
      val rules = RuleLoader.fromJsonString("""{"field": "id", "check_type": "is_unique"}""")
      rules should have size 1
      rules.head.checkType shouldBe "is_unique"
    }

    "return empty for null, empty, or blank input" in {
      RuleLoader.fromJsonString(null) shouldBe empty
      RuleLoader.fromJsonString("") shouldBe empty
      RuleLoader.fromJsonString("   ") shouldBe empty
    }

    "return empty for invalid JSON" in {
      RuleLoader.fromJsonString("not json {") shouldBe empty
    }

    "return empty for a non-object JSON value" in {
      RuleLoader.fromJsonString("[1, 2, 3]") shouldBe empty
      RuleLoader.fromJsonString("42") shouldBe empty
    }

    "parse numeric values as LongValue" in {
      val rules = RuleLoader.fromJsonString("""{"field": "age", "check_type": "is_greater_than", "value": 18}""")
      rules.head.value shouldBe Some(LongValue(18))
    }

    "parse string values" in {
      val rules = RuleLoader.fromJsonString("""{"field": "name", "check_type": "has_pattern", "value": "^[a-z]+$"}""")
      rules.head.value shouldBe Some(StringValue("^[a-z]+$"))
    }

    "parse boolean execute" in {
      val rules = RuleLoader.fromJsonString("""{"field": "id", "check_type": "is_complete", "execute": false}""")
      rules.head.execute shouldBe false
    }

    "preserve metadata" in {
      val rules = RuleLoader.fromJsonString("""{"field": "id", "check_type": "is_complete", "owner": "ops"}""")
      rules.head.metadata("owner") shouldBe "ops"
    }

    "throw when check_type is missing" in {
      an[SumehException] should be thrownBy RuleLoader.fromJsonString("""{"field": "id"}""")
    }
  }

  "RuleLoader.toJson" should {

    "emit one object per rule" in {
      import upickle.default._
      val json = RuleLoader.toJson(List(rule, rule))
      read[ujson.Arr](json).value should have size 2
    }

    "include the core fields" in {
      import upickle.default._
      val arr = read[ujson.Arr](RuleLoader.toJson(List(rule)))
      val obj = arr(0).obj
      (obj.keySet should contain).allOf("field", "check_type", "threshold", "execute", "level", "category")
      obj("threshold").num shouldBe 0.9
      obj("level").str shouldBe "ROW"
    }

    "serialize values with their type" in {
      import upickle.default._
      val r   = RuleDefinition.validated(Left("age"), "is_greater_than", value = Some(LongValue(18)))
      val arr = read[ujson.Arr](RuleLoader.toJson(List(r)))
      arr(0).obj("value").num shouldBe 18
    }

    "serialize list values as arrays" in {
      import upickle.default._
      val r = RuleDefinition.validated(
        Left("status"),
        "is_contained_in",
        value = Some(ListValue(List(StringValue("a"), StringValue("b"))))
      )
      val arr = read[ujson.Arr](RuleLoader.toJson(List(r)))
      arr(0).obj("value").arr.map(_.str) shouldBe List("a", "b")
    }

    "serialize metadata as strings" in {
      import upickle.default._
      val arr = read[ujson.Arr](RuleLoader.toJson(List(rule)))
      arr(0).obj("env").str shouldBe "prod"
    }

    "round-trip tolerance through JSON" in {
      import upickle.default._
      val r   = RuleDefinition.validated(Left("age"), "has_sum", value = Some(LongValue(100)), tolerance = 0.05)
      val arr = read[ujson.Arr](RuleLoader.toJson(List(r)))
      arr(0).obj("tolerance").num shouldBe 0.05

      val back = RuleLoader.fromJsonString(RuleLoader.toJson(List(r)))
      back.head.tolerance shouldBe 0.05
    }
  }
}
