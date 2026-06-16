package io.galileostd.sumeh.validation

import io.galileostd.sumeh.rule.{RuleDefinition, StringValue}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValidationSpec extends AnyWordSpec with Matchers {

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  def makeResult(
                  checkType: String = "is_complete",
                  field: String = "email",
                  status: ValidationStatus = ValidationStatus.PASS,
                  passRate: Option[Double] = Some(1.0),
                  violatingRowIds: List[Long] = List.empty,
                  message: Option[String] = None,
                  level: ValidationLevel = ValidationLevel.ROW,
                  category: String = "completeness"
                ): ValidationResult = ValidationResult(
    checkType       = checkType,
    field           = Left(field),
    status          = status,
    passRate        = passRate,
    violatingRowIds = violatingRowIds,
    message         = message,
    level           = level,
    category        = category
  )

  def makeReport(
                  results: List[ValidationResult] = List.empty,
                  totalRows: Long = 100L,
                  engine: String = "spark"
                ): ValidationReport[Nothing] = ValidationReport(
    results         = results,
    totalRows       = totalRows,
    executionTimeMs = 12.5,
    engine          = engine
  )

  // -------------------------------------------------------------------------
  // ValidationStatus
  // -------------------------------------------------------------------------

  "ValidationStatus" should {

    "have correct string representations" in {
      ValidationStatus.PASS.toString  shouldBe "PASS"
      ValidationStatus.FAIL.toString  shouldBe "FAIL"
      ValidationStatus.ERROR.toString shouldBe "ERROR"
    }
  }

  // -------------------------------------------------------------------------
  // ValidationLevel
  // -------------------------------------------------------------------------

  "ValidationLevel" should {

    "have correct string representations" in {
      ValidationLevel.ROW.toString   shouldBe "ROW"
      ValidationLevel.TABLE.toString shouldBe "TABLE"
    }

    "parse from string" in {
      ValidationLevel.fromString("ROW")   shouldBe ValidationLevel.ROW
      ValidationLevel.fromString("TABLE") shouldBe ValidationLevel.TABLE
    }

    "throw on unknown level" in {
      an[IllegalArgumentException] should be thrownBy {
        ValidationLevel.fromString("UNKNOWN")
      }
    }
  }

  // -------------------------------------------------------------------------
  // ValidationResult
  // -------------------------------------------------------------------------

  "ValidationResult" should {

    "have unique ids" in {
      val r1 = ValidationResult()
      val r2 = ValidationResult()
      r1.id should not equal r2.id
    }

    "default status to ERROR" in {
      ValidationResult().status shouldBe ValidationStatus.ERROR
    }

    "default violatingRowIds to empty" in {
      ValidationResult().violatingRowIds shouldBe empty
    }

    "default metadata to empty" in {
      ValidationResult().metadata shouldBe empty
    }

    "default passRate to None" in {
      ValidationResult().passRate shouldBe None
    }

    "default message to None" in {
      ValidationResult().message shouldBe None
    }

    "expose fieldName for single field" in {
      val r = makeResult(field = "email")
      r.fieldName shouldBe "email"
    }

    "have meaningful toString" in {
      val r = makeResult(checkType = "is_complete", field = "email")
      r.toString should include("is_complete")
      r.toString should include("email")
    }

    "include status in toString" in {
      val r = makeResult(status = ValidationStatus.FAIL)
      r.toString should include("FAIL")
    }
  }

  // -------------------------------------------------------------------------
  // ValidationReport — filtering properties
  // -------------------------------------------------------------------------

  "ValidationReport.passed" should {

    "return only PASS results" in {
      val results = List(
        makeResult(status = ValidationStatus.PASS),
        makeResult(status = ValidationStatus.FAIL),
        makeResult(status = ValidationStatus.PASS)
      )
      makeReport(results).passed should have size 2
    }
  }

  "ValidationReport.failed" should {

    "return only FAIL results" in {
      val results = List(
        makeResult(status = ValidationStatus.PASS),
        makeResult(status = ValidationStatus.FAIL),
        makeResult(status = ValidationStatus.FAIL)
      )
      makeReport(results).failed should have size 2
    }
  }

  "ValidationReport.errors" should {

    "return only ERROR results" in {
      val results = List(
        makeResult(status = ValidationStatus.PASS),
        makeResult(status = ValidationStatus.ERROR)
      )
      makeReport(results).errors should have size 1
    }
  }

  // -------------------------------------------------------------------------
  // ValidationReport — pass rate
  // -------------------------------------------------------------------------

  "ValidationReport.passRate" should {

    "be 1.0 when all pass" in {
      val results = List.fill(5)(makeResult(status = ValidationStatus.PASS))
      makeReport(results).passRate shouldBe 1.0
    }

    "be 0.0 when all fail" in {
      val results = List.fill(4)(makeResult(status = ValidationStatus.FAIL))
      makeReport(results).passRate shouldBe 0.0
    }

    "be 0.5 for mixed results" in {
      val results = List(
        makeResult(status = ValidationStatus.PASS),
        makeResult(status = ValidationStatus.PASS),
        makeResult(status = ValidationStatus.FAIL),
        makeResult(status = ValidationStatus.FAIL)
      )
      makeReport(results).passRate shouldBe 0.5
    }

    "be 1.0 for empty results" in {
      makeReport(List.empty).passRate shouldBe 1.0
    }
  }

  // -------------------------------------------------------------------------
  // ValidationReport — summary
  // -------------------------------------------------------------------------

  "ValidationReport.summary" should {

    "return a map" in {
      makeReport(List(makeResult())).summary() shouldBe a[Map[_, _]]
    }

    "contain required keys" in {
      val s = makeReport(List(makeResult())).summary()
      Seq("timestamp", "engine", "total_rows", "pass_rate", "passed", "failed", "errors", "validations")
        .foreach(k => s should contain key k)
    }

    "count correctly" in {
      val results = List(
        makeResult(status = ValidationStatus.PASS),
        makeResult(status = ValidationStatus.FAIL),
        makeResult(status = ValidationStatus.ERROR)
      )
      val s = makeReport(results).summary()
      s("passed") shouldBe 1
      s("failed") shouldBe 1
      s("errors") shouldBe 1
      s("total_validations") shouldBe 3
    }

    "cap sample violating ids" in {
      val ids    = (0L until 200L).toList
      val result = makeResult(status = ValidationStatus.FAIL, violatingRowIds = ids)
      val s      = makeReport(List(result)).summary(maxSampleIds = 50)
      val validations = s("validations").asInstanceOf[List[Map[String, Any]]]
      validations.head("sample_violating_ids").asInstanceOf[List[_]] should have size 50
    }

    "preserve engine name" in {
      makeReport(engine = "spark").summary()("engine") shouldBe "spark"
    }

    "preserve total_rows" in {
      makeReport(totalRows = 9999L).summary()("total_rows") shouldBe 9999L
    }
  }

  // -------------------------------------------------------------------------
  // ValidationReport — size / isEmpty
  // -------------------------------------------------------------------------

  "ValidationReport" should {

    "report correct size" in {
      val results = List.fill(4)(makeResult())
      makeReport(results).size shouldBe 4
    }

    "report isEmpty correctly" in {
      makeReport(List.empty).isEmpty shouldBe true
      makeReport(List(makeResult())).isEmpty shouldBe false
    }

    "have meaningful toString" in {
      val results = List(
        makeResult(status = ValidationStatus.PASS),
        makeResult(status = ValidationStatus.FAIL)
      )
      val str = makeReport(results).toString
      str should include("2")
    }
  }
}