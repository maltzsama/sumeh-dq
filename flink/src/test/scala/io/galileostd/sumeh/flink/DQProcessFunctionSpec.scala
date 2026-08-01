package io.galileostd.sumeh.flink

import io.galileostd.sumeh.flink.internal.DQProcessFunction
import io.galileostd.sumeh.rule.{ ListValue, RuleDefinition, StringValue }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DQProcessFunctionSpec extends AnyWordSpec with Matchers {

  private def evaluate(values: Map[String, Any], rules: Seq[RuleDefinition]): (List[String], List[String]) =
    DQProcessFunction.evaluate(values, rules)

  private val base = Map("name" -> "alice", "age" -> 30, "status" -> "active", "dt" -> "2024-05-06")

  "DQProcessFunction" should {

    "pass a complete field" in {
      val rules             = Seq(RuleDefinition.validated(Left("name"), "is_complete"))
      val (errors, skipped) = evaluate(base, rules)
      errors shouldBe empty
      skipped shouldBe empty
    }

    "flag a null for is_complete" in {
      val rules       = Seq(RuleDefinition.validated(Left("name"), "is_complete"))
      val (errors, _) = evaluate(base + ("name" -> null), rules)
      errors should have size 1
    }

    "flag values below threshold for is_positive" in {
      val rules       = Seq(RuleDefinition.validated(Left("age"), "is_positive"))
      val (errors, _) = evaluate(base + ("age" -> -5), rules)
      errors should have size 1
    }

    "flag values outside is_between" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules = Seq(
        RuleDefinition.validated(
          Left("age"),
          "is_between",
          value = Some(ListValue(List(LongValue(18), LongValue(65)))),
          threshold = 1.0
        )
      )
      val (errors, _) = evaluate(base + ("age" -> 70), rules)
      errors should have size 1
    }

    "skip uniqueness rules with a reason instead of passing silently" in {
      val rules             = Seq(RuleDefinition.validated(Left("name"), "is_unique"))
      val (errors, skipped) = evaluate(base, rules)
      errors shouldBe empty
      skipped should have size 1
      skipped.head should include("is_unique")
    }

    "skip TABLE-level rules with a reason" in {
      val rules             = Seq(RuleDefinition.validated(Left("age"), "has_mean"))
      val (errors, skipped) = evaluate(base, rules)
      errors shouldBe empty
      skipped should have size 1
      skipped.head should include("TABLE-level")
    }

    "skip satisfies (not implemented for the streaming engine)" in {
      val rules = Seq(RuleDefinition.validated(Left("name"), "satisfies", value = Some(StringValue("name = 'alice'"))))
      val (errors, skipped) = evaluate(base, rules)
      errors shouldBe empty
      skipped should have size 1
      skipped.head should include("satisfies")
    }

    "validate date format" in {
      val ok =
        Seq(RuleDefinition.validated(Left("dt"), "validate_date_format", value = Some(StringValue("yyyy-MM-dd"))))
      val (e1, _) = evaluate(base, ok)
      e1 shouldBe empty

      val bad =
        Seq(RuleDefinition.validated(Left("dt"), "validate_date_format", value = Some(StringValue("dd/MM/yyyy"))))
      val (e2, _) = evaluate(base, bad)
      e2 should have size 1
    }

    "not flag null for validate_date_format" in {
      val rules =
        Seq(RuleDefinition.validated(Left("dt"), "validate_date_format", value = Some(StringValue("yyyy-MM-dd"))))
      val (errors, _) = evaluate(base + ("dt" -> null), rules)
      errors shouldBe empty
    }
  }
}
