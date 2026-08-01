package io.galileostd.sumeh.flink

import io.galileostd.sumeh.flink.internal.DQProcessFunction
import io.galileostd.sumeh.rule.{ DoubleValue, ListValue, LongValue, RuleDefinition, StringValue }
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

  "numeric rules" should {

    "flag is_negative for non-negative values" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_negative"))
      evaluate(base + ("age" -> -5), rules)._1 shouldBe empty
      evaluate(base + ("age" -> 30), rules)._1 should have size 1
    }

    "flag is_equal on mismatch" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_equal", value = Some(LongValue(30))))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("age" -> 25), rules)._1 should have size 1
    }

    "flag is_greater_than on low values" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_greater_than", value = Some(LongValue(18))))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("age" -> 17), rules)._1 should have size 1
    }

    "flag is_less_than on high values" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_less_than", value = Some(LongValue(40))))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("age" -> 45), rules)._1 should have size 1
    }

    "flag is_greater_or_equal_than below the bound" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_greater_or_equal_than", value = Some(LongValue(17))))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("age" -> 16), rules)._1 should have size 1
    }

    "flag is_less_or_equal_than above the bound" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_less_or_equal_than", value = Some(LongValue(30))))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("age" -> 31), rules)._1 should have size 1
    }

    "compare is_equal_than against another column" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_equal_than", value = Some(StringValue("ref"))))
      evaluate(base + ("ref" -> 30), rules)._1 shouldBe empty
      evaluate(base + ("ref" -> 31), rules)._1 should have size 1
    }

    "flag is_in_millions below 1M" in {
      val rules = Seq(RuleDefinition.validated(Left("revenue"), "is_in_millions"))
      evaluate(base + ("revenue" -> 1500000), rules)._1 shouldBe empty
      evaluate(base + ("revenue" -> 500), rules)._1 should have size 1
    }

    "flag is_in_billions below 1B" in {
      val rules = Seq(RuleDefinition.validated(Left("revenue"), "is_in_billions"))
      evaluate(base + ("revenue" -> 3000000000L), rules)._1 shouldBe empty
      evaluate(base + ("revenue" -> 1500000), rules)._1 should have size 1
    }
  }

  "membership rules" should {

    "flag is_contained_in outside the set" in {
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "is_contained_in",
          value = Some(ListValue(List(StringValue("active"), StringValue("pending"))))
        )
      )
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("status" -> "deleted"), rules)._1 should have size 1
    }

    "treat is_in as an alias of is_contained_in" in {
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "is_in",
          value = Some(ListValue(List(StringValue("active"), StringValue("pending"))))
        )
      )
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("status" -> "deleted"), rules)._1 should have size 1
    }

    "flag not_contained_in when the value is present" in {
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "not_contained_in",
          value = Some(ListValue(List(StringValue("active"))))
        )
      )
      evaluate(base, rules)._1 should have size 1
      evaluate(base + ("status" -> "banned"), rules)._1 shouldBe empty
    }

    "treat not_in as an alias of not_contained_in" in {
      val rules = Seq(
        RuleDefinition.validated(Left("status"), "not_in", value = Some(ListValue(List(StringValue("active")))))
      )
      evaluate(base, rules)._1 should have size 1
      evaluate(base + ("status" -> "banned"), rules)._1 shouldBe empty
    }
  }

  "string rules" should {

    "flag has_pattern on a non-matching value" in {
      val rules = Seq(RuleDefinition.validated(Left("name"), "has_pattern", value = Some(StringValue("^[a-z]+$"))))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("name" -> "Alice!"), rules)._1 should have size 1
    }

    "flag is_legit for blank values" in {
      val rules = Seq(RuleDefinition.validated(Left("name"), "is_legit"))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("name" -> "  "), rules)._1 should have size 1
      evaluate(base + ("name" -> null), rules)._1 should have size 1
    }
  }

  "date rules" should {

    "flag is_today for any date other than today" in {
      val today     = java.time.LocalDate.now()
      val yesterday = today.minusDays(1)
      val rules     = Seq(RuleDefinition.validated(Left("dt"), "is_today"))
      evaluate(base + ("dt" -> today.toString), rules)._1 shouldBe empty
      evaluate(base + ("dt" -> yesterday.toString), rules)._1 should have size 1
    }

    "flag is_yesterday except for yesterday" in {
      val today     = java.time.LocalDate.now()
      val yesterday = today.minusDays(1)
      val rules     = Seq(RuleDefinition.validated(Left("dt"), "is_yesterday"))
      evaluate(base + ("dt" -> yesterday.toString), rules)._1 shouldBe empty
      evaluate(base + ("dt" -> today.toString), rules)._1 should have size 1
    }

    "flag is_t_minus_2 and is_t_minus_3" in {
      val today = java.time.LocalDate.now()
      Seq("is_t_minus_2", "is_t_minus_3").foreach {
        ruleType =>
          val rules = Seq(RuleDefinition.validated(Left("dt"), ruleType))
          evaluate(base + ("dt" -> today.minusDays(ruleType.last.toString.toInt).toString), rules)._1 shouldBe empty
          evaluate(base + ("dt" -> today.toString), rules)._1 should have size 1
      }
    }

    "flag is_past_date for today or future" in {
      val rules = Seq(RuleDefinition.validated(Left("dt"), "is_past_date"))
      evaluate(base + ("dt" -> "2020-01-01"), rules)._1 shouldBe empty
      evaluate(base + ("dt" -> "2099-12-31"), rules)._1 should have size 1
    }

    "flag is_future_date for past dates" in {
      val rules = Seq(RuleDefinition.validated(Left("dt"), "is_future_date"))
      evaluate(base + ("dt" -> "2099-12-31"), rules)._1 shouldBe empty
      evaluate(base + ("dt" -> "2020-01-01"), rules)._1 should have size 1
    }

    "flag is_on_weekday for weekends" in {
      val rules = Seq(RuleDefinition.validated(Left("dt"), "is_on_weekday"))
      evaluate(base + ("dt" -> "2024-05-06"), rules)._1 shouldBe empty // Monday
      evaluate(base + ("dt" -> "2024-05-11"), rules)._1 should have size 1 // Saturday
    }

    "flag is_on_weekend for weekdays" in {
      val rules = Seq(RuleDefinition.validated(Left("dt"), "is_on_weekend"))
      evaluate(base + ("dt" -> "2024-05-11"), rules)._1 shouldBe empty // Saturday
      evaluate(base + ("dt" -> "2024-05-06"), rules)._1 should have size 1 // Monday
    }

    "flag each is_on_<weekday> rule" in {
      val week = Map(
        "is_on_monday"    -> "2024-05-06",
        "is_on_tuesday"   -> "2024-05-07",
        "is_on_wednesday" -> "2024-05-08",
        "is_on_thursday"  -> "2024-05-09",
        "is_on_friday"    -> "2024-05-10",
        "is_on_saturday"  -> "2024-05-11",
        "is_on_sunday"    -> "2024-05-12"
      )
      week.foreach {
        case (ruleType, matchDate) =>
          val mismatch = java.time.LocalDate.parse(matchDate).plusDays(1).toString
          val rules    = Seq(RuleDefinition.validated(Left("dt"), ruleType))
          evaluate(base + ("dt" -> matchDate), rules)._1 shouldBe empty
          evaluate(base + ("dt" -> mismatch), rules)._1 should have size 1
      }
    }

    "flag is_date_between outside the range" in {
      val rules = Seq(
        RuleDefinition.validated(
          Left("dt"),
          "is_date_between",
          value = Some(ListValue(List(StringValue("2024-01-01"), StringValue("2024-12-31"))))
        )
      )
      evaluate(base + ("dt" -> "2024-05-06"), rules)._1 shouldBe empty
      evaluate(base + ("dt" -> "2025-01-01"), rules)._1 should have size 1
    }

    "flag is_date_after on or before the target" in {
      val rules = Seq(RuleDefinition.validated(Left("dt"), "is_date_after", value = Some(StringValue("2024-01-01"))))
      evaluate(base + ("dt" -> "2024-05-06"), rules)._1 shouldBe empty
      evaluate(base + ("dt" -> "2023-12-31"), rules)._1 should have size 1
    }

    "flag is_date_before on or after the target" in {
      val rules = Seq(RuleDefinition.validated(Left("dt"), "is_date_before", value = Some(StringValue("2024-12-31"))))
      evaluate(base + ("dt" -> "2024-05-06"), rules)._1 shouldBe empty
      evaluate(base + ("dt" -> "2025-01-01"), rules)._1 should have size 1
    }
  }

  "edge cases" should {

    "flag are_complete when any field is null" in {
      val rules = Seq(RuleDefinition.validated(Right(List("name", "age")), "are_complete"))
      evaluate(base, rules)._1 shouldBe empty
      evaluate(base + ("name" -> null), rules)._1 should have size 1
    }

    "not flag null for non-completeness numeric rules" in {
      val rules = Seq(RuleDefinition.validated(Left("age"), "is_positive"))
      evaluate(base + ("age" -> null), rules)._1 shouldBe empty
    }

    "surface an ERROR for a non-numeric value on a numeric rule" in {
      val rules       = Seq(RuleDefinition.validated(Left("age"), "is_positive"))
      val (errors, _) = evaluate(base + ("age" -> "abc"), rules)
      errors should have size 1
      errors.head should include("ERROR[is_positive]")
    }

    "surface an ERROR for an unparseable date on a date rule" in {
      val rules       = Seq(RuleDefinition.validated(Left("dt"), "is_past_date"))
      val (errors, _) = evaluate(base + ("dt" -> "not-a-date"), rules)
      errors should have size 1
      errors.head should include("ERROR[is_past_date]")
    }
  }
}
