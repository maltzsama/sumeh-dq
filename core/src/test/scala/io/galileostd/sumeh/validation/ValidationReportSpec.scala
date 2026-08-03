package io.galileostd.sumeh.validation

import io.galileostd.sumeh.engine.Splittable
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValidationReportSpec extends AnyWordSpec with Matchers {

  private def result(status: ValidationStatus): ValidationResult =
    ValidationResult(checkType = "is_complete", field = Left("email"), status = status)

  "ValidationReport passRate" should {

    "be 1.0 when every result passes" in {
      val report = ValidationReport[Unit](List(result(ValidationStatus.PASS)), 10, 1.0, "spark")
      report.passRate shouldBe 1.0
    }

    "count failures against the evaluated set" in {
      val report =
        ValidationReport[Unit](List(result(ValidationStatus.PASS), result(ValidationStatus.FAIL)), 10, 1.0, "spark")
      report.passRate shouldBe 0.5
    }

    "exclude skipped results from the denominator" in {
      val report = ValidationReport[Unit](
        List(result(ValidationStatus.PASS), result(ValidationStatus.SKIPPED), result(ValidationStatus.SKIPPED)),
        10,
        1.0,
        "spark"
      )
      report.passRate shouldBe 1.0
    }

    "return 1.0 when nothing is evaluated" in {
      val report = ValidationReport[Unit](List(result(ValidationStatus.SKIPPED)), 10, 1.0, "spark")
      report.passRate shouldBe 1.0
    }

    "return 1.0 for an empty report" in {
      val report = ValidationReport[Unit](List.empty, 0, 0.0, "spark")
      report.passRate shouldBe 1.0
    }
  }

  "ValidationReport summary" should {

    "count results per status" in {
      val report = ValidationReport[Unit](
        List(
          result(ValidationStatus.PASS),
          result(ValidationStatus.FAIL),
          result(ValidationStatus.ERROR),
          result(ValidationStatus.SKIPPED)
        ),
        10,
        2.5,
        "flink"
      )
      val s = report.summary()
      s("passed") shouldBe 1
      s("failed") shouldBe 1
      s("errors") shouldBe 1
      s("skipped") shouldBe 1
      s("pass_rate") shouldBe 1.0 / 3.0
      s("engine") shouldBe "flink"
      s("total_rows") shouldBe 10L
      s("execution_time_ms") shouldBe 2.5
    }

    "report fail_count from metadata" in {
      val report = ValidationReport[Unit](
        List(
          ValidationResult(
            checkType = "is_complete",
            field = Left("email"),
            status = ValidationStatus.FAIL,
            metadata = Map("fail_count" -> 10L)
          )
        ),
        10,
        1.0,
        "spark"
      )
      val validation = report
        .summary()("validations")
        .asInstanceOf[List[_]]
        .head
        .asInstanceOf[Map[String, Any]]
      validation("fail_count") shouldBe 10L
    }

    "expose null fail_count when metadata is absent" in {
      val report = ValidationReport[Unit](
        List(ValidationResult(checkType = "is_complete", field = Left("email"), status = ValidationStatus.FAIL)),
        10,
        1.0,
        "spark"
      )
      val validation = report
        .summary()("validations")
        .asInstanceOf[List[_]]
        .head
        .asInstanceOf[Map[String, Any]]
      validation("fail_count") == null shouldBe true
    }

    "not break the summary on unexpected metadata" in {
      val report = ValidationReport[Unit](
        List(
          ValidationResult(
            checkType = "x",
            field = Left("col"),
            status = ValidationStatus.FAIL,
            metadata = Map("fail_count" -> "nao-e-numero")
          )
        ),
        1L,
        1.0,
        "spark"
      )
      noException should be thrownBy report.summary()
      val validation = report
        .summary()("validations")
        .asInstanceOf[List[_]]
        .head
        .asInstanceOf[Map[String, Any]]
      validation("fail_count") == null shouldBe true
    }

    "expose nullable fields as null" in {
      val report = ValidationReport[Unit](List(result(ValidationStatus.PASS)), 1, 1.0, "spark")
      val validation = report
        .summary()("validations")
        .asInstanceOf[List[_]]
        .head
        .asInstanceOf[Map[String, Any]]
      validation("expected") == null shouldBe true
      validation("actual") == null shouldBe true
      validation("message") == null shouldBe true
    }
  }

  "ValidationReport" should {

    "bucket results by status" in {
      val report = ValidationReport[Unit](
        List(result(ValidationStatus.PASS), result(ValidationStatus.FAIL), result(ValidationStatus.SKIPPED)),
        10,
        1.0,
        "spark"
      )
      report.passed should have size 1
      report.failed should have size 1
      report.skipped should have size 1
      report.errors shouldBe empty
    }

    "report size and emptiness" in {
      ValidationReport[Unit](List(result(ValidationStatus.PASS)), 1, 1.0, "spark").size shouldBe 1
      ValidationReport[Unit](List.empty, 0, 0.0, "spark").isEmpty shouldBe true
    }

    "throw when split is requested without a dataframe" in {
      val report = ValidationReport[Unit](List(result(ValidationStatus.PASS)), 1, 1.0, "spark")
      an[IllegalStateException] should be thrownBy report.split()(splittableForUnit)
    }

    "include skipped in toString" in {
      val report = ValidationReport[Unit](List(result(ValidationStatus.SKIPPED)), 1, 1.0, "spark")
      report.toString should include("pass_rate=1.00")
    }

    "instantiate ValidationResult.skipped with a reason" in {
      val r =
        ValidationResult.skipped("is_unique", Left("id"), ValidationLevel.ROW, "uniqueness", "streaming unsupported")
      r.status shouldBe ValidationStatus.SKIPPED
      r.message shouldBe Some("Skipped: streaming unsupported")
      r.checkType shouldBe "is_unique"
    }
  }

  private val splittableForUnit: Splittable[Unit] = new Splittable[Unit] {
    def split(df: Unit): (Unit, Unit) = ((), ())
  }
}
