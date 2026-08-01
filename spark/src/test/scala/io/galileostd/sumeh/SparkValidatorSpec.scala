package io.galileostd.sumeh

import java.sql.Date
import java.time.LocalDate

import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.spark.SparkValidator
import io.galileostd.sumeh.validation.ValidationStatus
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class SparkValidatorSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-spark")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  def dfBasic = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row(1, "alice", 30, "active", 1500000.0),
        Row(2, "bob", 25, "inactive", 500.0),
        Row(3, null, 40, "active", 2000000.0),
        Row(4, "diana", 17, "pending", 3000000000.0),
        Row(5, "eve", 35, "active", 800000.0)
      )
    ),
    StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("revenue", DoubleType, nullable = true)
      )
    )
  )

  def dfUnique = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row(1, "a"),
        Row(2, "b"),
        Row(3, "a"),
        Row(4, "c")
      )
    ),
    StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("category", StringType, nullable = true)
      )
    )
  )

  def dfDates = {
    val today     = LocalDate.now()
    val yesterday = today.minusDays(1)
    val past      = LocalDate.of(2020, 1, 1)
    val future    = LocalDate.of(2099, 12, 31)
    val monday    = LocalDate.of(2025, 6, 2)
    val saturday  = LocalDate.of(2025, 6, 7)

    spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(today.toString),
          Row(yesterday.toString),
          Row(past.toString),
          Row(future.toString),
          Row(monday.toString),
          Row(saturday.toString)
        )
      ),
      StructType(Seq(StructField("dt", StringType, nullable = true)))
    )
  }

  // -------------------------------------------------------------------------
  // Completeness
  // -------------------------------------------------------------------------

  "Completeness" should {

    "pass for a complete column" in {
      val rules  = Seq(RuleDefinition.validated(Left("id"), "is_complete", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail for a column with nulls" in {
      val rules  = Seq(RuleDefinition.validated(Left("name"), "is_complete", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "fail for multi-field completeness with any null" in {
      val rules  = Seq(RuleDefinition.validated(Right(List("id", "name")), "are_complete", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "reflect actual pass rate below 1.0" in {
      val rules  = Seq(RuleDefinition.validated(Left("name"), "is_complete", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      val result = report.results.head
      result.status shouldBe ValidationStatus.FAIL
      result.actualValue.get should be < 1.0
    }
  }

  // -------------------------------------------------------------------------
  // Uniqueness
  // -------------------------------------------------------------------------

  "Uniqueness" should {

    "pass for a unique column" in {
      val rules  = Seq(RuleDefinition.validated(Left("id"), "is_unique", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail for a non-unique column" in {
      val rules  = Seq(RuleDefinition.validated(Left("category"), "is_unique", threshold = 1.0))
      val report = SparkValidator.validate(dfUnique, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "pass for is_primary_key alias" in {
      val rules  = Seq(RuleDefinition.validated(Left("id"), "is_primary_key", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "pass for composite key (are_unique)" in {
      val rules  = Seq(RuleDefinition.validated(Right(List("id", "category")), "are_unique", threshold = 1.0))
      val report = SparkValidator.validate(dfUnique, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }
  }

  // -------------------------------------------------------------------------
  // Comparison
  // -------------------------------------------------------------------------

  "Comparison" should {

    "pass is_positive for positive ages" in {
      val rules  = Seq(RuleDefinition.validated(Left("age"), "is_positive", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "pass is_greater_than 0" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules =
        Seq(RuleDefinition.validated(Left("age"), "is_greater_than", value = Some(LongValue(0)), threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail is_greater_than 18 when age=17 exists" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules =
        Seq(RuleDefinition.validated(Left("age"), "is_greater_than", value = Some(LongValue(18)), threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "pass is_between [0, 150]" in {
      import io.galileostd.sumeh.rule.{ ListValue, LongValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("age"),
          "is_between",
          value = Some(ListValue(List(LongValue(0), LongValue(150)))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail is_in_millions when some values are below 1M" in {
      val rules  = Seq(RuleDefinition.validated(Left("revenue"), "is_in_millions", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }
  }

  // -------------------------------------------------------------------------
  // Membership
  // -------------------------------------------------------------------------

  "Membership" should {

    "pass is_contained_in with valid set" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "is_contained_in",
          value = Some(ListValue(List(StringValue("active"), StringValue("inactive"), StringValue("pending")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail is_contained_in with restricted set" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "is_contained_in",
          value = Some(ListValue(List(StringValue("active")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "pass not_contained_in with absent values" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "not_contained_in",
          value = Some(ListValue(List(StringValue("banned"), StringValue("deleted")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }
  }

  // -------------------------------------------------------------------------
  // Pattern
  // -------------------------------------------------------------------------

  "Pattern" should {

    "pass has_pattern with threshold allowing nulls" in {
      import io.galileostd.sumeh.rule.StringValue
      val rules = Seq(
        RuleDefinition.validated(Left("name"), "has_pattern", value = Some(StringValue("^[a-z]+$")), threshold = 0.7)
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail is_legit when column has nulls" in {
      val rules  = Seq(RuleDefinition.validated(Left("name"), "is_legit", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }
  }

  // -------------------------------------------------------------------------
  // Date
  // -------------------------------------------------------------------------

  "Date" should {

    "detect past dates" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_past_date", threshold = 0.4))
      val report = SparkValidator.validate(dfDates, rules)
      report.results.head.actualValue.get should be > 0.0
    }

    "detect future dates" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_future_date", threshold = 0.1))
      val report = SparkValidator.validate(dfDates, rules)
      report.results.head.actualValue.get should be > 0.0
    }

    "detect weekend dates" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_on_weekend", threshold = 0.1))
      val report = SparkValidator.validate(dfDates, rules)
      report.results.head.actualValue.get should be > 0.0
    }

    "detect monday" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_on_monday", threshold = 0.1))
      val report = SparkValidator.validate(dfDates, rules)
      report.results.head.actualValue.get should be > 0.0
    }
  }

  // -------------------------------------------------------------------------
  // Aggregation (TABLE level)
  // -------------------------------------------------------------------------

  "Aggregation" should {

    "pass has_mean with correct value" in {
      import io.galileostd.sumeh.rule.DoubleValue
      val rules =
        Seq(RuleDefinition.validated(Left("age"), "has_mean", value = Some(DoubleValue(29.4)), threshold = 0.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "pass has_min" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules  = Seq(RuleDefinition.validated(Left("age"), "has_min", value = Some(LongValue(17)), threshold = 0.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "pass has_max" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules  = Seq(RuleDefinition.validated(Left("age"), "has_max", value = Some(LongValue(40)), threshold = 0.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "pass has_cardinality" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules =
        Seq(RuleDefinition.validated(Left("status"), "has_cardinality", value = Some(LongValue(3)), threshold = 0.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }
  }

  // -------------------------------------------------------------------------
  // ValidationReport
  // -------------------------------------------------------------------------

  "ValidationReport" should {

    "return a report with engine=spark" in {
      val rules  = Seq(RuleDefinition.validated(Left("id"), "is_complete", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report should not be null
      report.results should have size 1
      report.engine shouldBe "spark"
    }

    "have pass_rate between 0 and 1" in {
      val rules = Seq(
        RuleDefinition.validated(Left("id"), "is_complete", threshold = 1.0),
        RuleDefinition.validated(Left("name"), "is_complete", threshold = 1.0)
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.passRate should ((be >= 0.0).and(be <= 1.0))
    }

    "split into good and bad DataFrames" in {
      val rules       = Seq(RuleDefinition.validated(Left("name"), "is_complete", threshold = 1.0))
      val report      = SparkValidator.validate(dfBasic, rules)
      val (good, bad) = report.dfValidated.get.splitByErrors()
      good should not be null
      bad should not be null
    }

    "set execution_time_ms > 0" in {
      val rules  = Seq(RuleDefinition.validated(Left("id"), "is_complete", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.executionTimeMs should be > 0.0
    }

    "return one result per rule" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(Left("id"), "is_complete", threshold = 1.0),
        RuleDefinition.validated(Left("id"), "is_unique", threshold = 1.0),
        RuleDefinition.validated(Left("age"), "is_positive", threshold = 1.0),
        RuleDefinition.validated(
          Left("status"),
          "is_contained_in",
          value = Some(ListValue(List(StringValue("active"), StringValue("inactive"), StringValue("pending")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results should have size 4
    }

    "produce ERROR result for unknown field" in {
      val rules  = Seq(RuleDefinition.validated(Left("nonexistent"), "is_complete", threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.ERROR
    }

    "not include table-level rules in row bifurcation" in {
      import io.galileostd.sumeh.rule.DoubleValue
      val rules =
        Seq(RuleDefinition.validated(Left("age"), "has_mean", value = Some(DoubleValue(29.4)), threshold = 0.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results should have size 1
    }
  }

  // -------------------------------------------------------------------------
  // Schema validation (TABLE level)
  // -------------------------------------------------------------------------

  "Schema validation" should {

    import io.galileostd.sumeh.rule.StringValue

    "fail when a column type does not match the contract" in {
      val schemaJson = """{"name": "integer"}"""
      val rules = Seq(
        RuleDefinition.validated(Left("*"), "validate_schema", value = Some(StringValue(schemaJson)), threshold = 1.0)
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "pass when the schema matches the contract" in {
      val schemaJson =
        """{"id": "integer", "name": "string", "age": "integer", "status": "string", "revenue": "float"}"""
      val rules = Seq(
        RuleDefinition.validated(Left("*"), "validate_schema", value = Some(StringValue(schemaJson)), threshold = 1.0)
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }
  }

  // -------------------------------------------------------------------------
  // Bifurcation
  // -------------------------------------------------------------------------

  "Bifurcation" should {

    "populate _dq_errors only for failing rows" in {
      val rules       = Seq(RuleDefinition.validated(Left("name"), "is_complete", threshold = 1.0))
      val report      = SparkValidator.validate(dfBasic, rules)
      val (good, bad) = report.dfValidated.get.splitByErrors()
      bad.count() shouldBe 1
      good.count() shouldBe 4
      val errs = bad
        .select(org.apache.spark.sql.functions.size(org.apache.spark.sql.functions.col("_dq_errors")).alias("n"))
        .collect()(0)
        .getAs[Int]("n")
      errs shouldBe 1
    }

    "split() via the implicit Splittable" in {
      val rules       = Seq(RuleDefinition.validated(Left("name"), "is_complete", threshold = 1.0))
      val report      = SparkValidator.validate(dfBasic, rules)
      val (good, bad) = report.split()
      good.count() shouldBe 4
      bad.count() shouldBe 1
    }
  }

  // -------------------------------------------------------------------------
  // Comparison against typed values
  // -------------------------------------------------------------------------

  "Comparison with typed values" should {

    "evaluate is_equal against a DateValue" in {
      import io.galileostd.sumeh.rule.DateValue
      import java.time.LocalDate

      val dfDatesTyped = spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(java.sql.Date.valueOf("2020-01-01")),
            Row(java.sql.Date.valueOf("2021-06-01"))
          )
        ),
        StructType(Seq(StructField("dt", DateType, nullable = true)))
      )

      val rules = Seq(
        RuleDefinition.validated(
          Left("dt"),
          "is_equal",
          value = Some(DateValue(LocalDate.of(2020, 1, 1))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfDatesTyped, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      report.results.head.actualValue.get should be < 1.0
    }
  }

  // -------------------------------------------------------------------------
  // Skipped rules
  // -------------------------------------------------------------------------

  "Skipped rules" should {

    "mark execute=false rules as SKIPPED without lowering pass rate" in {
      val rules  = Seq(RuleDefinition.validated(Left("id"), "is_complete", threshold = 1.0, execute = false))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.SKIPPED
      report.passRate shouldBe 1.0
      report.skipped should have size 1
    }
  }
}
