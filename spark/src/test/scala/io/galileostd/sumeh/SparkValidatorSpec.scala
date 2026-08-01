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

  def dfColumnCompare = spark.createDataFrame(
    spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 2), Row(3, 4))),
    StructType(Seq(StructField("a", IntegerType, nullable = true), StructField("b", IntegerType, nullable = true)))
  )

  def dfScale = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(Row(5000000000.0), Row(3000000000.0), Row(1500000000.0), Row(500000000.0), Row(2000000000.0))
    ),
    StructType(Seq(StructField("revenue", DoubleType, nullable = true)))
  )

  def dfNegatives = spark.createDataFrame(
    spark.sparkContext.parallelize(Seq(Row(-5), Row(3))),
    StructType(Seq(StructField("n", IntegerType, nullable = true)))
  )

  def dfKeyPairs = spark.createDataFrame(
    spark.sparkContext.parallelize(Seq(Row(1, "a"), Row(1, "a"), Row(2, "b"))),
    StructType(Seq(StructField("id", IntegerType, nullable = true), StructField("code", StringType, nullable = true)))
  )

  def dfTodayMinus = {
    val today = LocalDate.now()
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(today.toString),
          Row(today.minusDays(1).toString),
          Row(today.minusDays(2).toString),
          Row(today.minusDays(3).toString)
        )
      ),
      StructType(Seq(StructField("dt", StringType, nullable = true)))
    )
  }

  def dfWeekdays = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row("2025-06-02"), // Monday
        Row("2025-06-03"), // Tuesday
        Row("2025-06-04"), // Wednesday
        Row("2025-06-05"), // Thursday
        Row("2025-06-06"), // Friday
        Row("2025-06-07"), // Saturday
        Row("2025-06-08")  // Sunday
      )
    ),
    StructType(Seq(StructField("dt", StringType, nullable = true)))
  )

  def dfDateRange = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(Row("2020-01-01"), Row("2021-06-01"), Row("2022-01-01"))
    ),
    StructType(Seq(StructField("dt", StringType, nullable = true)))
  )

  def dfMixedDates = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row("2024-01-15"),
        Row("2024-02-30"), // impossible calendar date
        Row("not-a-date"),
        Row(null)
      )
    ),
    StructType(Seq(StructField("dt", StringType, nullable = true)))
  )

  def dfUniform = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(Row("a"), Row("a"), Row("b"), Row("b"))
    ),
    StructType(Seq(StructField("category", StringType, nullable = true)))
  )

  def dfConstant = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(Row("x"), Row("x"), Row("x"), Row("x"))
    ),
    StructType(Seq(StructField("category", StringType, nullable = true)))
  )

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

    "pass for is_composite_key on unique pairs" in {
      val rules  = Seq(RuleDefinition.validated(Right(List("id", "category")), "is_composite_key", threshold = 1.0))
      val report = SparkValidator.validate(dfUnique, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail is_composite_key on duplicate pairs" in {
      val rules  = Seq(RuleDefinition.validated(Right(List("id", "code")), "is_composite_key", threshold = 1.0))
      val report = SparkValidator.validate(dfKeyPairs, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
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

    "pass is_less_than 41" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules =
        Seq(RuleDefinition.validated(Left("age"), "is_less_than", value = Some(LongValue(41)), threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail is_less_than 30 when age=30 exists" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules =
        Seq(RuleDefinition.validated(Left("age"), "is_less_than", value = Some(LongValue(30)), threshold = 1.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      report.results.head.actualValue.get shouldBe 0.4
    }

    "pass is_less_or_equal_than 40" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules = Seq(
        RuleDefinition.validated(Left("age"), "is_less_or_equal_than", value = Some(LongValue(40)), threshold = 1.0)
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "pass is_greater_or_equal_than 17" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules = Seq(
        RuleDefinition.validated(Left("age"), "is_greater_or_equal_than", value = Some(LongValue(17)), threshold = 1.0)
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail is_equal_than when columns differ on a row" in {
      import io.galileostd.sumeh.rule.StringValue
      val rules = Seq(
        RuleDefinition.validated(Left("a"), "is_equal_than", value = Some(StringValue("b")), threshold = 1.0)
      )
      val report = SparkValidator.validate(dfColumnCompare, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      report.results.head.actualValue.get shouldBe 2.0 / 3.0
    }

    "pass is_equal_than when columns match on every row" in {
      import io.galileostd.sumeh.rule.StringValue
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 2))),
        StructType(Seq(StructField("a", IntegerType, nullable = true), StructField("b", IntegerType, nullable = true)))
      )
      val rules = Seq(
        RuleDefinition.validated(Left("a"), "is_equal_than", value = Some(StringValue("b")), threshold = 1.0)
      )
      SparkValidator.validate(df, rules).results.head.status shouldBe ValidationStatus.PASS
    }

    "pass is_negative for negative values" in {
      val rules  = Seq(RuleDefinition.validated(Left("n"), "is_negative", threshold = 0.5))
      val report = SparkValidator.validate(dfNegatives, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 0.5
    }

    "fail is_negative when positive values exist" in {
      val rules  = Seq(RuleDefinition.validated(Left("n"), "is_negative", threshold = 1.0))
      val report = SparkValidator.validate(dfNegatives, rules)
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

    "pass is_in as an alias for is_contained_in" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "is_in",
          value = Some(ListValue(List(StringValue("active"), StringValue("inactive"), StringValue("pending")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "pass not_in with absent values" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "not_in",
          value = Some(ListValue(List(StringValue("banned"), StringValue("deleted")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "fail not_in when a value is present" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("status"),
          "not_in",
          value = Some(ListValue(List(StringValue("active")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      report.results.head.actualValue.get shouldBe 0.4
    }

    "pass is_in_billions for values >= 1B" in {
      val rules  = Seq(RuleDefinition.validated(Left("revenue"), "is_in_billions", threshold = 0.8))
      val report = SparkValidator.validate(dfScale, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 0.8
    }

    "fail is_in_billions when some values are below 1B" in {
      val rules  = Seq(RuleDefinition.validated(Left("revenue"), "is_in_billions", threshold = 1.0))
      val report = SparkValidator.validate(dfScale, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
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

    "not throw on unparseable dates (Spark 4 ANSI)" in {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row("2024-05-06"), Row("not-a-date"), Row("2024/05/06"), Row("2099-01-01"))
        ),
        StructType(Seq(StructField("dt", StringType, nullable = true)))
      )
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_past_date", threshold = 1.0))
      val report = SparkValidator.validate(df, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      val (good, bad) = report.split()
      good.count() shouldBe 3 // past + unparseable rows are not failures
      bad.count() shouldBe 1  // only the future date fails
    }

    "fail validate_date_format on malformed strings" in {
      import io.galileostd.sumeh.rule.StringValue
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row("2024-05-06"), Row("2024/05/06"), Row("abc"))
        ),
        StructType(Seq(StructField("dt", StringType, nullable = true)))
      )
      val rules = Seq(
        RuleDefinition.validated(
          Left("dt"),
          "validate_date_format",
          value = Some(StringValue("yyyy-MM-dd")),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(df, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      report.split()._2.count() shouldBe 2
    }

    "detect is_today" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_today", threshold = 0.1))
      val report = SparkValidator.validate(dfTodayMinus, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 0.25
    }

    "detect is_yesterday" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_yesterday", threshold = 0.1))
      val report = SparkValidator.validate(dfTodayMinus, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 0.25
    }

    "detect is_t_minus_2" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_t_minus_2", threshold = 0.1))
      val report = SparkValidator.validate(dfTodayMinus, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "detect is_t_minus_3" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_t_minus_3", threshold = 0.1))
      val report = SparkValidator.validate(dfTodayMinus, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "detect is_on_weekday" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_on_weekday", threshold = 0.5))
      val report = SparkValidator.validate(dfWeekdays, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 5.0 / 7.0
    }

    "detect is_on_weekend" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_on_weekend", threshold = 0.1))
      val report = SparkValidator.validate(dfWeekdays, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 2.0 / 7.0
    }

    "detect each weekday rule" in {
      val rules = Seq(
        RuleDefinition.validated(Left("dt"), "is_on_monday", threshold = 0.1),
        RuleDefinition.validated(Left("dt"), "is_on_tuesday", threshold = 0.1),
        RuleDefinition.validated(Left("dt"), "is_on_wednesday", threshold = 0.1),
        RuleDefinition.validated(Left("dt"), "is_on_thursday", threshold = 0.1),
        RuleDefinition.validated(Left("dt"), "is_on_friday", threshold = 0.1),
        RuleDefinition.validated(Left("dt"), "is_on_saturday", threshold = 0.1),
        RuleDefinition.validated(Left("dt"), "is_on_sunday", threshold = 0.1)
      )
      val report = SparkValidator.validate(dfWeekdays, rules)
      report.results.foreach(r => r.status shouldBe ValidationStatus.PASS)
      report.results.foreach(r => r.actualValue.get shouldBe 1.0 / 7.0)
    }

    "detect is_date_between" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val rules = Seq(
        RuleDefinition.validated(
          Left("dt"),
          "is_date_between",
          value = Some(ListValue(List(StringValue("2020-06-01"), StringValue("2021-12-31")))),
          threshold = 0.1
        )
      )
      val report = SparkValidator.validate(dfDateRange, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 1.0 / 3.0
    }

    "detect is_date_after" in {
      import io.galileostd.sumeh.rule.StringValue
      val rules = Seq(
        RuleDefinition.validated(Left("dt"), "is_date_after", value = Some(StringValue("2021-01-01")), threshold = 0.1)
      )
      val report = SparkValidator.validate(dfDateRange, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 2.0 / 3.0
    }

    "detect is_date_before" in {
      import io.galileostd.sumeh.rule.StringValue
      val rules = Seq(
        RuleDefinition.validated(Left("dt"), "is_date_before", value = Some(StringValue("2021-06-01")), threshold = 0.1)
      )
      val report = SparkValidator.validate(dfDateRange, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 1.0 / 3.0
    }

    "not throw on unparseable dates for is_date_between" in {
      import io.galileostd.sumeh.rule.{ ListValue, StringValue }
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row("2024-05-06"), Row("not-a-date"), Row("2024/05/06"), Row("2025-01-01"))
        ),
        StructType(Seq(StructField("dt", StringType, nullable = true)))
      )
      val rules = Seq(
        RuleDefinition.validated(
          Left("dt"),
          "is_date_between",
          value = Some(ListValue(List(StringValue("2024-01-01"), StringValue("2024-12-31")))),
          threshold = 1.0
        )
      )
      val report = SparkValidator.validate(df, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      // unparseable dates are treated as pass (null), only the out-of-range row fails
      report.split()._2.count() shouldBe 1
      report.split()._1.count() shouldBe 3
    }

    "not throw on unparseable dates for is_on_weekday" in {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row("2024-05-06"), Row("not-a-date"), Row("2024/05/06"), Row("2025-06-08"))
        ),
        StructType(Seq(StructField("dt", StringType, nullable = true)))
      )
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_on_weekday", threshold = 1.0))
      val report = SparkValidator.validate(df, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "pass all_date_checks when every date is valid" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "all_date_checks", threshold = 1.0))
      val report = SparkValidator.validate(dfDateRange, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 1.0
    }

    "fail all_date_checks on unparseable and impossible dates (nulls skip)" in {
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "all_date_checks", threshold = 1.0))
      val report = SparkValidator.validate(dfMixedDates, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
      report.results.head.actualValue.get shouldBe 0.5
      report.split()._1.count() shouldBe 2 // real calendar date + null pass
      report.split()._2.count() shouldBe 2 // impossible date and garbage fail
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

    "pass has_sum with the sum of the column" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules  = Seq(RuleDefinition.validated(Left("age"), "has_sum", value = Some(LongValue(147)), threshold = 0.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 147.0
    }

    "fail has_sum on wrong value" in {
      import io.galileostd.sumeh.rule.LongValue
      val rules  = Seq(RuleDefinition.validated(Left("age"), "has_sum", value = Some(LongValue(999)), threshold = 0.0))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "pass has_std within relative tolerance" in {
      import io.galileostd.sumeh.rule.DoubleValue
      val rules =
        Seq(RuleDefinition.validated(Left("age"), "has_std", value = Some(DoubleValue(8.9)), threshold = 0.01))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get should be > 8.9
      report.results.head.actualValue.get should be < 8.92
    }

    "pass has_entropy for a uniform column (H = 1.0)" in {
      import io.galileostd.sumeh.rule.DoubleValue
      val rules =
        Seq(
          RuleDefinition.validated(Left("category"), "has_entropy", value = Some(DoubleValue(1.0)), threshold = 0.001)
        )
      val report = SparkValidator.validate(dfUniform, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 1.0 +- 0.001
    }

    "fail has_entropy on a wrong expected value" in {
      import io.galileostd.sumeh.rule.DoubleValue
      val rules =
        Seq(RuleDefinition.validated(Left("category"), "has_entropy", value = Some(DoubleValue(0.5)), threshold = 0.0))
      val report = SparkValidator.validate(dfUniform, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }

    "pass has_infogain for a uniform column (normalized H = 1.0)" in {
      import io.galileostd.sumeh.rule.DoubleValue
      val rules = Seq(
        RuleDefinition.validated(Left("category"), "has_infogain", value = Some(DoubleValue(1.0)), threshold = 0.001)
      )
      val report = SparkValidator.validate(dfUniform, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 1.0 +- 0.001
    }

    "pass has_infogain for a constant column (H / log2(n) = 0.0)" in {
      import io.galileostd.sumeh.rule.DoubleValue
      val rules = Seq(
        RuleDefinition.validated(Left("category"), "has_infogain", value = Some(DoubleValue(0.0)), threshold = 0.0)
      )
      val report = SparkValidator.validate(dfConstant, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 0.0 +- 1e-9
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

  // -------------------------------------------------------------------------
  // Threshold boundary
  // -------------------------------------------------------------------------

  "Threshold" should {

    "pass when pass rate equals the threshold" in {
      // name has 4/5 non-null rows -> pass rate 0.8
      val rules  = Seq(RuleDefinition.validated(Left("name"), "is_complete", threshold = 0.8))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.results.head.actualValue.get shouldBe 0.8
    }

    "fail when pass rate drops just below the threshold" in {
      val rules  = Seq(RuleDefinition.validated(Left("name"), "is_complete", threshold = 0.8001))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.FAIL
    }
  }

  // -------------------------------------------------------------------------
  // Empty input
  // -------------------------------------------------------------------------

  "Empty input" should {

    "not fail an empty DataFrame for completeness" in {
      val empty = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(Seq(StructField("id", IntegerType, nullable = true)))
      )
      val rules  = Seq(RuleDefinition.validated(Left("id"), "is_complete", threshold = 1.0))
      val report = SparkValidator.validate(empty, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
      report.totalRows shouldBe 0L
    }

    "not fail an empty DataFrame for a date rule" in {
      val empty = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(Seq(StructField("dt", StringType, nullable = true)))
      )
      val rules  = Seq(RuleDefinition.validated(Left("dt"), "is_past_date", threshold = 1.0))
      val report = SparkValidator.validate(empty, rules)
      report.results.head.status shouldBe ValidationStatus.PASS
    }

    "produce ERROR for a missing field on a numeric rule" in {
      val rules  = Seq(RuleDefinition.validated(Left("missing"), "is_greater_than"))
      val report = SparkValidator.validate(dfBasic, rules)
      report.results.head.status shouldBe ValidationStatus.ERROR
    }
  }
}
