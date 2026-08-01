package io.galileostd.sumeh.validation

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValidationLevelSpec extends AnyWordSpec with Matchers {

  "ValidationLevel.fromString" should {

    "parse canonical values" in {
      ValidationLevel.fromString("ROW") shouldBe ValidationLevel.ROW
      ValidationLevel.fromString("TABLE") shouldBe ValidationLevel.TABLE
    }

    "be case-insensitive" in {
      ValidationLevel.fromString("row") shouldBe ValidationLevel.ROW
      ValidationLevel.fromString("Table") shouldBe ValidationLevel.TABLE
    }

    "throw on unknown values" in {
      an[IllegalArgumentException] should be thrownBy ValidationLevel.fromString("BATCH")
      an[IllegalArgumentException] should be thrownBy ValidationLevel.fromString("")
    }
  }

  "ValidationStatus" should {

    "have stable string forms" in {
      ValidationStatus.PASS.toString shouldBe "PASS"
      ValidationStatus.FAIL.toString shouldBe "FAIL"
      ValidationStatus.ERROR.toString shouldBe "ERROR"
      ValidationStatus.SKIPPED.toString shouldBe "SKIPPED"
    }

    "be distinct values" in {
      val all = Set[ValidationStatus](
        ValidationStatus.PASS,
        ValidationStatus.FAIL,
        ValidationStatus.ERROR,
        ValidationStatus.SKIPPED
      )
      all should have size 4
    }
  }
}
