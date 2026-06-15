package io.galileostudio.sumeh.rule

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RuleRegistrySpec extends AnyWordSpec with Matchers {

  "RuleRegistry" should {

    "have rules loaded" in {
      RuleRegistry.listRules().size should be > 0
    }

    "return only strings from listRules" in {
      RuleRegistry.listRules().foreach(_ shouldBe a[String])
    }

    "contain all known rules" in {
      val known = Seq(
        "is_complete", "are_complete", "is_unique", "are_unique",
        "is_between", "is_positive", "is_negative",
        "is_contained_in", "not_contained_in",
        "has_pattern", "is_legit",
        "is_today", "is_past_date", "is_future_date",
        "has_mean", "has_min", "has_max", "has_sum", "has_cardinality",
        "satisfies", "validate_schema"
      )
      val registered = RuleRegistry.listRules()
      known.foreach { rule =>
        registered should contain(rule)
      }
    }

    "return a RuleEntry for known rule" in {
      val entry = RuleRegistry.getRule("is_complete")
      entry shouldBe defined
    }

    "return None for unknown rule" in {
      RuleRegistry.getRule("totally_fake_rule_xyz") shouldBe None
    }

    "have correct level for ROW rules" in {
      RuleRegistry.getRule("is_complete").get.level shouldBe "ROW"
    }

    "have correct level for TABLE rules" in {
      RuleRegistry.getRule("has_mean").get.level shouldBe "TABLE"
    }

    "have correct category for completeness" in {
      RuleRegistry.getRule("is_complete").get.category shouldBe "completeness"
    }

    "support spark engine for standard rules" in {
      RuleRegistry.isSupported("is_complete", "spark") shouldBe true
    }

    "support flink engine for ROW rules" in {
      RuleRegistry.isSupported("is_complete", "flink") shouldBe true
    }

    "not support unknown engine" in {
      RuleRegistry.isSupported("is_complete", "oracle") shouldBe false
    }

    "not support unknown rule for any engine" in {
      RuleRegistry.isSupported("fake_rule", "spark") shouldBe false
    }

    "not support streaming engines for TABLE rules" in {
      RuleRegistry.isSupported("has_mean", "spark-streaming") shouldBe false
      RuleRegistry.isSupported("has_mean", "flink-streaming") shouldBe false
    }

    "support batch engines for TABLE rules" in {
      RuleRegistry.isSupported("has_mean", "spark") shouldBe true
      RuleRegistry.isSupported("has_mean", "flink") shouldBe true
    }

    "contain all alias rules" in {
      val aliases = Seq("is_primary_key", "is_composite_key", "is_in", "not_in", "is_yesterday")
      aliases.foreach { alias =>
        RuleRegistry.getRule(alias) shouldBe defined
      }
    }

    "return rules by category" in {
      val completeness = RuleRegistry.byCategory("completeness")
      completeness should not be empty
      completeness.foreach(_.category shouldBe "completeness")
    }

    "return rules by level" in {
      val tableRules = RuleRegistry.byLevel("TABLE")
      tableRules should not be empty
      tableRules.foreach(_.level shouldBe "TABLE")
    }
  }
}