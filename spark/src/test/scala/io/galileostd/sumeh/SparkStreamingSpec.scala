package io.galileostd.sumeh

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }

import io.galileostd.sumeh.rule.{ RuleDefinition, StringValue }
import io.galileostd.sumeh.spark.SparkValidator
import io.galileostd.sumeh.validation.ValidationStatus
import org.apache.spark.sql.{ functions => F, SparkSession }
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class SparkStreamingSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("sumeh-test-streaming")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private val schema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("dt", StringType, nullable = true)
    )
  )

  private val rules = Seq(
    RuleDefinition.validated(Left("name"), "is_complete"),
    RuleDefinition.validated(Left("age"), "is_positive"),
    RuleDefinition.validated(Left("dt"), "validate_date_format", value = Some(StringValue("yyyy-MM-dd"))),
    RuleDefinition.validated(Left("id"), "is_unique"),
    RuleDefinition.validated(Left("age"), "has_mean"),
    RuleDefinition.validated(Left("name"), "satisfies", value = Some(StringValue("name = 'x'")))
  )

  private val rows = Seq(
    """{"id":1,"name":"alice","age":30,"status":"active","dt":"2024-05-06"}""",
    """{"id":2,"name":null,"age":25,"status":"inactive","dt":"2024-05-06"}""",
    """{"id":3,"name":"bob","age":-5,"status":"pending","dt":"2024/05/06"}"""
  )

  private def writeRows(rows: String*): String = {
    val dir = Files.createTempDirectory("sumeh-stream").toString
    Files.write(Paths.get(dir, "data.json"), rows.mkString("\n").getBytes(StandardCharsets.UTF_8))
    dir
  }

  private def streamOf(dir: String) = spark.readStream.schema(schema).json(dir)

  "SparkValidator on a streaming DataFrame" should {

    "report engine=spark-streaming and skip unsupported rules" in {
      val report = SparkValidator.validate(streamOf(writeRows(rows: _*)), rules)

      report.engine shouldBe "spark-streaming"
      report.totalRows shouldBe -1L

      val skipped = report.results.filter(_.status == ValidationStatus.SKIPPED)
      (skipped.map(_.checkType) should contain).allOf("is_unique", "has_mean", "satisfies")

      // executed row rules are reported as applied (no pass rate on a stream)
      (report.results.map(_.checkType) should contain).allOf("is_complete", "is_positive", "validate_date_format")
      report.results.filter(_.checkType == "is_complete").head.status shouldBe ValidationStatus.PASS
      report.results.filter(_.checkType == "is_complete").head.passRate shouldBe None
    }

    "annotate _dq_errors and _dq_skipped in a single pass" in {
      val report = SparkValidator.validate(streamOf(writeRows(rows: _*)), rules)

      val q = report.dfValidated.get.toNative.writeStream
        .format("memory")
        .queryName("dq_out")
        .outputMode("append")
        .start()
      q.processAllAvailable()
      q.stop()

      val out = spark.sql("select * from dq_out")
      (out.columns should contain).allOf("_dq_errors", "_dq_skipped")
      out.count() shouldBe 3
      out.filter(F.size(F.col("_dq_errors")) > 0).count() shouldBe 2
      out.filter(F.size(F.col("_dq_errors")) === 0).count() shouldBe 1

      val skipped = out.select("_dq_skipped").head().getString(0)
      skipped should include("is_unique")
      skipped should include("has_mean")
      skipped should include("satisfies")
    }

    "bifurcate into good and bad streams" in {
      val report      = SparkValidator.validate(streamOf(writeRows(rows: _*)), rules)
      val (good, bad) = report.dfValidated.get.splitByErrors()

      val gq = good.writeStream.format("memory").queryName("dq_good").outputMode("append").start()
      gq.processAllAvailable()
      gq.stop()
      spark.sql("select * from dq_good").count() shouldBe 1

      val bq = bad.writeStream.format("memory").queryName("dq_bad").outputMode("append").start()
      bq.processAllAvailable()
      bq.stop()
      spark.sql("select * from dq_bad").count() shouldBe 2
    }

    "not fail a stream on unparseable dates (Spark 4 ANSI)" in {
      val report = SparkValidator.validate(
        streamOf(writeRows(rows: _*)),
        Seq(RuleDefinition.validated(Left("dt"), "is_past_date"))
      )

      val q = report.dfValidated.get.toNative.writeStream
        .format("memory")
        .queryName("dq_date_out")
        .outputMode("append")
        .start()
      q.processAllAvailable()
      q.stop()

      spark.sql("select * from dq_date_out").count() shouldBe 3
    }

    "bifurcate into good and bad streams when every row is valid" in {
      val allGood = Seq(
        """{"id":1,"name":"alice","age":30,"status":"active","dt":"2024-05-06"}""",
        """{"id":2,"name":"bob","age":25,"status":"active","dt":"2024-05-06"}""",
        """{"id":3,"name":"carol","age":40,"status":"active","dt":"2024-05-06"}"""
      )
      val report = SparkValidator.validate(
        streamOf(writeRows(allGood: _*)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )
      val (good, bad) = report.dfValidated.get.splitByErrors()

      val gq = good.writeStream.format("memory").queryName("dq_good_all").outputMode("append").start()
      gq.processAllAvailable()
      gq.stop()
      spark.sql("select * from dq_good_all").count() shouldBe 3

      val bq = bad.writeStream.format("memory").queryName("dq_bad_all").outputMode("append").start()
      bq.processAllAvailable()
      bq.stop()
      spark.sql("select * from dq_bad_all").count() shouldBe 0
    }

    "bifurcate into good and bad streams when every row is invalid" in {
      val allBad = Seq(
        """{"id":1,"name":null,"age":-5,"status":"active","dt":"2024-05-06"}""",
        """{"id":2,"name":null,"age":-1,"status":"active","dt":"2024-05-06"}"""
      )
      val report = SparkValidator.validate(
        streamOf(writeRows(allBad: _*)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )
      val (good, bad) = report.dfValidated.get.splitByErrors()

      val gq = good.writeStream.format("memory").queryName("dq_good_none").outputMode("append").start()
      gq.processAllAvailable()
      gq.stop()
      spark.sql("select * from dq_good_none").count() shouldBe 0

      val bq = bad.writeStream.format("memory").queryName("dq_bad_none").outputMode("append").start()
      bq.processAllAvailable()
      bq.stop()
      spark.sql("select * from dq_bad_none").count() shouldBe 2
    }

    "correlate a failing row with the result that flagged it in streaming" in {
      val report = SparkValidator.validate(
        streamOf(writeRows(rows: _*)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )

      val q = report.dfValidated.get.toNative.writeStream
        .format("memory")
        .queryName("dq_corr")
        .outputMode("append")
        .start()
      q.processAllAvailable()
      q.stop()

      val out = spark.sql("select * from dq_corr")
      val bad = out.filter(F.size(F.col("_dq_errors")) > 0)
      val idOnRow = bad
        .select(F.col("_dq_errors")(0)("rule_id"))
        .collect()(0)
        .getString(0)

      report.results.filter(_.checkType == "is_complete").map(_.id).toSet should contain(idOnRow)
    }
  }
}
