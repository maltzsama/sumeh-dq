package io.galileostd.sumeh.rule

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RuleValueImplicitsSpec extends AnyWordSpec with Matchers {

  "RuleValue implicits" should {

    "convert scalar types" in {
      (18: RuleValue) shouldBe LongValue(18L)
      (65L: RuleValue) shouldBe LongValue(65L)
      (3.14: RuleValue) shouldBe DoubleValue(3.14)
      ("active": RuleValue) shouldBe StringValue("active")
      (true: RuleValue) shouldBe BoolValue(true)
    }

    "convert a List of plain values" in {
      (List(18, 65): RuleValue) shouldBe ListValue(List(LongValue(18L), LongValue(65L)))
      (List("a", "b"): RuleValue) shouldBe ListValue(List(StringValue("a"), StringValue("b")))
    }

    "build a mixed list via RuleValue.of" in {
      RuleValue.of(18, "active") shouldBe ListValue(List(LongValue(18L), StringValue("active")))
    }

    "build a rule with plain-typed value inside Some" in {
      val r = RuleDefinition.validated(Left("age"), "is_between", value = Some(List(18, 65)))
      r.value shouldBe Some(ListValue(List(LongValue(18L), LongValue(65L))))
    }

    "let old-style explicit constructors still compile" in {
      val r = RuleDefinition.validated(
        Left("status"),
        "is_contained_in",
        value = Some(ListValue(List(StringValue("active"), StringValue("inactive"))))
      )
      r.value shouldBe Some(ListValue(List(StringValue("active"), StringValue("inactive"))))
    }
  }
}
