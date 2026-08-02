package io.galileostd.sumeh.metric

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricResultSpec extends AnyWordSpec with Matchers {

  "MetricResult" should {

    "default affectedRowIds to empty" in {
      val m = MetricResult("completeness", Left("email"), 0.9, 100)
      m.affectedRowIds shouldBe empty
    }

    "default metadata to empty" in {
      val m = MetricResult("completeness", Left("email"), 0.9, 100)
      m.metadata shouldBe empty
    }

    "expose fieldName for a single field" in {
      MetricResult("x", Left("email"), 0.9, 10).fieldName shouldBe "email"
    }

    "join fieldName for multiple fields" in {
      MetricResult("x", Right(List("id", "name")), 0.9, 10).fieldName shouldBe "id,name"
    }

    "have a meaningful toString" in {
      val str = MetricResult("completeness", Left("email"), 0.9, 100).toString
      str should include("completeness")
      str should include("email")
    }
  }
}
