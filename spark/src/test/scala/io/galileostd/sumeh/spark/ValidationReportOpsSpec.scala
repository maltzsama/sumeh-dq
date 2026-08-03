package io.galileostd.sumeh.spark

import io.galileostd.sumeh.spark.ValidationReportOps._
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationReport, ValidationResult, ValidationStatus }
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class ValidationReportOpsSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-validation-report-ops")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private def someResult: ValidationResult =
    ValidationResult(
      checkType = "is_complete",
      field = Left("email"),
      level = ValidationLevel.ROW,
      category = "completeness",
      status = ValidationStatus.PASS,
      passRate = Some(1.0),
      expectedValue = Some(1.0),
      actualValue = Some(1.0),
      message = Some("all good"),
      metadata = Map("fail_count" -> 0L)
    )

  "ValidationReportOps.toDataFrame" should {

    "produce one row per validation result" in {
      val report = ValidationReport[Unit](List(someResult, someResult, someResult), 10, 1.0, "spark")
      report.toDataFrame.count() shouldBe report.results.size
    }

    "match the canonical schema exactly" in {
      val report = ValidationReport[Unit](List(someResult), 10, 1.0, "spark")
      report.toDataFrame.schema shouldBe ValidationReportOps.schema
    }

    "share the same run_id across every row" in {
      val report = ValidationReport[Unit](List(someResult, someResult), 10, 1.0, "spark")
      report.toDataFrame.select("run_id").distinct().count() shouldBe 1
    }

    "return an empty DataFrame, not null, for a report with no results" in {
      val report = ValidationReport[Unit](List.empty, 0, 0.0, "spark")
      report.toDataFrame.count() shouldBe 0
      report.toDataFrame.schema shouldBe ValidationReportOps.schema
    }

    "write nulls, not structs, in nullable numeric columns" in {
      val bare = ValidationResult(
        checkType = "is_positive",
        field = Left("amount"),
        status = ValidationStatus.PASS
      )
      val report = ValidationReport[Unit](List(bare), 10, 1.0, "spark")
      val row    = report.toDataFrame.select("pass_rate", "expected", "actual", "fail_count").collect()(0)
      row.isNullAt(0) shouldBe true
      row.isNullAt(1) shouldBe true
      row.isNullAt(2) shouldBe true
      row.isNullAt(3) shouldBe true
    }

    "round-trip through parquet" in {
      val report = ValidationReport[Unit](List(someResult, someResult), 10, 1.0, "spark")
      val dir    = java.nio.file.Files.createTempDirectory("sumeh-dq-parquet").toString
      report.toDataFrame.write.mode("overwrite").parquet(dir)
      val back = spark.read.parquet(dir)
      back.schema.fields.map(f => (f.name, f.dataType)) shouldBe
      ValidationReportOps.schema.fields.map(f => (f.name, f.dataType))
      back.count() shouldBe report.results.size
      back.select("run_id").distinct().count() shouldBe 1
    }

    "keep failCount consistent with summary()" in {
      val result = ValidationResult(
        checkType = "is_complete",
        field = Left("email"),
        status = ValidationStatus.FAIL,
        metadata = Map("null_count" -> 3L)
      )
      val report       = ValidationReport[Unit](List(result), 10, 1.0, "spark")
      val dfCount      = report.toDataFrame.select("fail_count").collect()(0).getLong(0)
      val summaryRow   = report.summary()("validations").asInstanceOf[List[_]].head.asInstanceOf[Map[String, Any]]
      val summaryCount = summaryRow("fail_count")
      dfCount shouldBe 3L
      summaryCount shouldBe 3L
      dfCount shouldBe summaryCount
    }
  }
}
