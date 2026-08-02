package io.galileostd.sumeh.spark.expr

import java.sql.Date
import java.time.LocalDate

import io.galileostd.sumeh.rule._
import org.apache.spark.sql.{ functions => F, Row, SparkSession }
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class FailConditionSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-failcondition")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private val intSchema  = StructType(Seq(StructField("c", IntegerType, nullable = true)))
  private val strSchema  = StructType(Seq(StructField("c", StringType, nullable = true)))
  private val longSchema = StructType(Seq(StructField("c", LongType, nullable = true)))
  private def pairSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType)))
  private def dateSchema = StructType(Seq(StructField("dt", StringType, nullable = true)))

  private def toRows[A](items: Seq[A]): Seq[Row] = items.map(Row(_))
  private def df[A](items: Seq[A], schema: StructType = strSchema) =
    spark.createDataFrame(spark.sparkContext.parallelize(toRows(items)), schema)

  // -------------------------------------------------------------------------
  // Completeness
  // -------------------------------------------------------------------------

  "Completeness" should {

    "fail is_complete when field is null" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_complete")
      val data   = df[Any](Seq("a", null), strSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe null
    }

    "fail are_complete when any field is null" in {
      val rule = RuleDefinition.validated(Right(List("a", "b")), "are_complete")
      val data = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, null.asInstanceOf[Integer]), Row(2, 3))),
        pairSchema
      )
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }
  }

  // -------------------------------------------------------------------------
  // Uniqueness
  // -------------------------------------------------------------------------

  "Uniqueness" should {

    "fail is_unique when value is duplicated" in {
      val rule     = RuleDefinition.validated(Left("c"), "is_unique")
      val data     = df[Any](Seq(1, 1, 2), intSchema)
      val withCond = data.withColumn("_fc", FailCondition(rule))
      val failed   = withCond.filter(F.col("_fc")).drop("_fc").collect()
      failed should have size 2
    }

    "fail are_unique when a pair is duplicated" in {
      val rule = RuleDefinition.validated(Right(List("a", "b")), "are_unique")
      val data = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 2), Row(1, 2), Row(3, 4))),
        pairSchema
      )
      val withCond = data.withColumn("_fc", FailCondition(rule))
      val failed   = withCond.filter(F.col("_fc")).drop("_fc").collect()
      failed should have size 2
    }

    "fail is_primary_key as alias for is_unique" in {
      val rule     = RuleDefinition.validated(Left("c"), "is_primary_key")
      val data     = df[Any](Seq(1, 1, 3), intSchema)
      val withCond = data.withColumn("_fc", FailCondition(rule))
      val failed   = withCond.filter(F.col("_fc")).drop("_fc").collect()
      failed should have size 2
    }

    "fail is_composite_key as alias for are_unique" in {
      val rule = RuleDefinition.validated(Right(List("a", "b")), "is_composite_key")
      val data = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 1), Row(1, 1), Row(2, 3))),
        pairSchema
      )
      val withCond = data.withColumn("_fc", FailCondition(rule))
      val failed   = withCond.filter(F.col("_fc")).drop("_fc").collect()
      failed should have size 2
    }
  }

  // -------------------------------------------------------------------------
  // Comparison
  // -------------------------------------------------------------------------

  "Comparison" should {

    "fail is_equal when value differs" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_equal", value = Some(LongValue(5)))
      val data   = df[Any](Seq(5, 3), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 3
    }

    "fail is_greater_than when value is not greater" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_greater_than", value = Some(LongValue(6)))
      val data   = df[Any](Seq(10, 5), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 5
    }

    "fail is_less_than when value is not smaller" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_less_than", value = Some(LongValue(3)))
      val data   = df[Any](Seq(1, 5), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 5
    }

    "fail is_greater_or_equal_than when lower than threshold" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_greater_or_equal_than", value = Some(LongValue(10)))
      val data   = df[Any](Seq(10, 5), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 5
    }

    "fail is_less_or_equal_than when higher than threshold" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_less_or_equal_than", value = Some(LongValue(5)))
      val data   = df[Any](Seq(5, 10), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 10
    }

    "fail is_positive when value is negative or zero" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_positive")
      val data   = df[Any](Seq(5, -1), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe -1
    }

    "fail is_negative when value is positive or zero" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_negative")
      val data   = df[Any](Seq(-5, 3), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 3
    }

    "fail is_in_millions when value is below 1M" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_in_millions")
      val data   = df[Any](Seq(2000000L, 500000L), longSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getLong(0) shouldBe 500000L
    }

    "fail is_in_billions when value is below 1B" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_in_billions")
      val data   = df[Any](Seq(2000000000L, 500000000L), longSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getLong(0) shouldBe 500000000L
    }

    "fail is_between when value is outside [min, max]" in {
      val rule = RuleDefinition.validated(
        Left("c"),
        "is_between",
        value = Some(ListValue(List(LongValue(5), LongValue(10))))
      )
      val data   = df[Any](Seq(7, 2), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 2
    }

    "fail is_equal_than when columns differ" in {
      val rule = RuleDefinition.validated(Left("a"), "is_equal_than", value = Some(StringValue("b")))
      val data = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 3))),
        pairSchema
      )
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 2
    }
  }

  // -------------------------------------------------------------------------
  // Membership
  // -------------------------------------------------------------------------

  "Membership" should {

    "fail is_contained_in when value is not in the allowed list" in {
      val rule = RuleDefinition.validated(
        Left("c"),
        "is_contained_in",
        value = Some(ListValue(List(StringValue("active"), StringValue("pending"))))
      )
      val data   = df[Any](Seq("active", "banned"), strSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "banned"
    }

    "fail not_contained_in when value is in the disallowed list" in {
      val rule = RuleDefinition.validated(
        Left("c"),
        "not_contained_in",
        value = Some(ListValue(List(StringValue("banned"), StringValue("deleted"))))
      )
      val data   = df[Any](Seq("active", "banned"), strSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "banned"
    }

    "fail is_in (alias) same as is_contained_in" in {
      val rule = RuleDefinition.validated(
        Left("c"),
        "is_in",
        value = Some(ListValue(List(StringValue("a"), StringValue("b"))))
      )
      val data   = df[Any](Seq("a", "x"), strSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "x"
    }

    "fail not_in (alias) same as not_contained_in" in {
      val rule = RuleDefinition.validated(
        Left("c"),
        "not_in",
        value = Some(ListValue(List(StringValue("banned"))))
      )
      val data   = df[Any](Seq("ok", "banned"), strSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "banned"
    }
  }

  // -------------------------------------------------------------------------
  // Pattern
  // -------------------------------------------------------------------------

  "Pattern" should {

    "fail has_pattern when value does not match regex" in {
      val rule   = RuleDefinition.validated(Left("c"), "has_pattern", value = Some(StringValue("[a-z]+")))
      val data   = df[Any](Seq("abc", "123"), strSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "123"
    }

    "fail is_legit when value is null" in {
      val rule   = RuleDefinition.validated(Left("c"), "is_legit")
      val data   = df[Any](Seq("abc", null), strSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe null
    }
  }

  // -------------------------------------------------------------------------
  // Date
  // -------------------------------------------------------------------------

  "Date" should {

    import java.time.LocalDate

    "fail is_today when date is not today" in {
      val today  = LocalDate.now()
      val rule   = RuleDefinition.validated(Left("dt"), "is_today")
      val data   = df[Any](Seq(today.toString, "2020-01-01"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2020-01-01"
    }

    "fail is_t_minus_1 when date is not yesterday" in {
      val today     = LocalDate.now()
      val yesterday = today.minusDays(1)
      val rule      = RuleDefinition.validated(Left("dt"), "is_t_minus_1")
      val data      = df[Any](Seq(yesterday.toString, today.toString), dateSchema)
      val failed    = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe today.toString
    }

    "fail is_yesterday as alias for is_t_minus_1" in {
      val today     = LocalDate.now()
      val yesterday = today.minusDays(1)
      val rule      = RuleDefinition.validated(Left("dt"), "is_yesterday")
      val data      = df[Any](Seq(yesterday.toString, today.toString), dateSchema)
      val failed    = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_t_minus_2 when date is not two days ago" in {
      val today  = LocalDate.now()
      val minus2 = today.minusDays(2)
      val rule   = RuleDefinition.validated(Left("dt"), "is_t_minus_2")
      val data   = df[Any](Seq(minus2.toString, today.toString), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_t_minus_3 when date is not three days ago" in {
      val today  = LocalDate.now()
      val minus3 = today.minusDays(3)
      val rule   = RuleDefinition.validated(Left("dt"), "is_t_minus_3")
      val data   = df[Any](Seq(minus3.toString, today.toString), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_past_date when date is future or today" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_past_date")
      val data   = df[Any](Seq("2020-01-01", "2099-12-31"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2099-12-31"
    }

    "fail is_future_date when date is past or today" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_future_date")
      val data   = df[Any](Seq("2099-12-31", "2020-01-01"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2020-01-01"
    }

    "fail is_date_between when date is outside [start, end]" in {
      val rule = RuleDefinition.validated(
        Left("dt"),
        "is_date_between",
        value = Some(ListValue(List(StringValue("2024-01-01"), StringValue("2024-12-31"))))
      )
      val data   = df[Any](Seq("2024-06-15", "2025-01-01"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2025-01-01"
    }

    "fail is_date_after when date is on or before target" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_date_after", value = Some(StringValue("2024-06-01")))
      val data   = df[Any](Seq("2024-07-01", "2024-01-01"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2024-01-01"
    }

    "fail is_date_before when date is on or after target" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_date_before", value = Some(StringValue("2024-06-01")))
      val data   = df[Any](Seq("2024-01-01", "2024-07-01"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2024-07-01"
    }

    "fail is_on_weekday when date is Saturday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_weekday")
      val data   = df[Any](Seq("2025-06-02", "2025-06-07"), dateSchema) // Monday vs Saturday
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2025-06-07"
    }

    "fail is_on_weekend when date is Monday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_weekend")
      val data   = df[Any](Seq("2025-06-07", "2025-06-02"), dateSchema) // Saturday vs Monday
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2025-06-02"
    }

    "fail is_on_monday when date is Tuesday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_monday")
      val data   = df[Any](Seq("2025-06-02", "2025-06-03"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2025-06-03"
    }

    "fail is_on_tuesday when date is Monday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_tuesday")
      val data   = df[Any](Seq("2025-06-03", "2025-06-02"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_on_wednesday when date is Monday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_wednesday")
      val data   = df[Any](Seq("2025-06-04", "2025-06-02"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_on_thursday when date is Monday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_thursday")
      val data   = df[Any](Seq("2025-06-05", "2025-06-02"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_on_friday when date is Monday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_friday")
      val data   = df[Any](Seq("2025-06-06", "2025-06-02"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_on_saturday when date is Monday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_saturday")
      val data   = df[Any](Seq("2025-06-07", "2025-06-02"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail is_on_sunday when date is Monday" in {
      val rule   = RuleDefinition.validated(Left("dt"), "is_on_sunday")
      val data   = df[Any](Seq("2025-06-08", "2025-06-02"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
    }

    "fail validate_date_format with unparseable string" in {
      val rule = RuleDefinition.validated(
        Left("dt"),
        "validate_date_format",
        value = Some(StringValue("yyyy-MM-dd"))
      )
      val data   = df[Any](Seq("2024-01-01", "abc"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "abc"
    }

    "fail all_date_checks on impossible date" in {
      val rule   = RuleDefinition.validated(Left("dt"), "all_date_checks")
      val data   = df[Any](Seq("2024-01-15", "2024-02-30"), dateSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getString(0) shouldBe "2024-02-30"
    }
  }

  // -------------------------------------------------------------------------
  // SQL
  // -------------------------------------------------------------------------

  "SQL" should {

    "fail satisfies when condition is false" in {
      val rule   = RuleDefinition.validated(Left("c"), "satisfies", value = Some(StringValue("c > 4")))
      val data   = df[Any](Seq(5, 3), intSchema)
      val failed = data.filter(FailCondition(rule)).collect()
      failed should have size 1
      failed(0).getInt(0) shouldBe 3
    }
  }

  // -------------------------------------------------------------------------
  // Value validation — throws on missing / wrong-type value
  // -------------------------------------------------------------------------

  "Value validation" should {

    "throw on is_equal without value" in {
      val rule = RuleDefinition.validated(Left("c"), "is_equal")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_greater_than without value" in {
      val rule = RuleDefinition.validated(Left("c"), "is_greater_than")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_less_than without value" in {
      val rule = RuleDefinition.validated(Left("c"), "is_less_than")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_greater_or_equal_than without value" in {
      val rule = RuleDefinition.validated(Left("c"), "is_greater_or_equal_than")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_less_or_equal_than without value" in {
      val rule = RuleDefinition.validated(Left("c"), "is_less_or_equal_than")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_between without value" in {
      val rule = RuleDefinition.validated(Left("c"), "is_between")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_equal_than without value" in {
      val rule = RuleDefinition.validated(Left("a"), "is_equal_than")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_contained_in without a list" in {
      val rule = RuleDefinition.validated(Left("c"), "is_contained_in", value = Some(StringValue("SP")))
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on not_contained_in without a list" in {
      val rule = RuleDefinition.validated(Left("c"), "not_contained_in", value = Some(StringValue("SP")))
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on has_pattern without value" in {
      val rule = RuleDefinition.validated(Left("c"), "has_pattern")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_date_between without value" in {
      val rule = RuleDefinition.validated(Left("dt"), "is_date_between")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_date_after without value" in {
      val rule = RuleDefinition.validated(Left("dt"), "is_date_after")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on is_date_before without value" in {
      val rule = RuleDefinition.validated(Left("dt"), "is_date_before")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on validate_date_format without value" in {
      val rule = RuleDefinition.validated(Left("dt"), "validate_date_format")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on satisfies without value" in {
      val rule = RuleDefinition.validated(Left("c"), "satisfies")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }

    "throw on unknown checkType" in {
      val rule = new RuleDefinition(Left("c"), "some_nonexistent_check")
      an[IllegalArgumentException] should be thrownBy FailCondition(rule)
    }
  }
}
