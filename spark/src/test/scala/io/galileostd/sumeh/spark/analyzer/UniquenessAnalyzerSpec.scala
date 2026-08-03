package io.galileostd.sumeh.spark.analyzer

import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.types._
import org.apache.spark.SparkListenerBusTestSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class UniquenessAnalyzerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-uniqueness-analyzer")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private def dfOf(rows: Seq[Row], fields: StructField*) =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(fields))

  private def rangeDf(n: Long): DataFrame = {
    val ss = spark
    import ss.implicits._
    ss.range(n).toDF("id")
  }

  private val intCol = StructField("id", IntegerType, nullable = true)
  private val strCol = StructField("code", StringType, nullable = true)

  "UniquenessAnalyzer" should {

    "compute uniqueness in a single Spark job" in {
      import org.apache.spark.scheduler.{ SparkListener, SparkListenerJobStart }

      class JobCounter extends SparkListener {
        @volatile var jobs: Int                                        = 0
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = jobs += 1
      }

      // Range-backed DataFrames trigger exactly one job per action on this
      // setup, so a plain scan is a trustworthy baseline for "one job".
      val df      = rangeDf(5)
      val counter = new JobCounter
      spark.sparkContext.addSparkListener(counter)
      try {
        df.count()
        SparkListenerBusTestSupport.waitUntilEmpty(spark.sparkContext, 10000)
        val baselineJobs = counter.jobs

        counter.jobs = 0
        UniquenessAnalyzer.analyze(df, RuleDefinition.validated(Left("id"), "is_unique"))
        SparkListenerBusTestSupport.waitUntilEmpty(spark.sparkContext, 10000)

        counter.jobs shouldBe baselineJobs
      } finally
        spark.sparkContext.removeSparkListener(counter)
    }

    "report uniqueness correctly on an empty DataFrame" in {
      val empty  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq(intCol)))
      val metric = UniquenessAnalyzer.analyze(empty, RuleDefinition.validated(Left("id"), "is_unique"))
      metric.totalRows shouldBe 0L
      metric.metadata("total_count") shouldBe 0L
      metric.metadata("duplicate_count") shouldBe 0L
      metric.value shouldBe 1.0
    }

    "keep duplicate and total counts identical to the previous implementation" in {
      val df     = dfOf(Seq(Row(1), Row(2), Row(1), Row(3)), intCol)
      val metric = UniquenessAnalyzer.analyze(df, RuleDefinition.validated(Left("id"), "is_unique"))
      metric.totalRows shouldBe 4L
      metric.metadata("total_count") shouldBe 4L
      metric.metadata("duplicate_count") shouldBe 2L
      metric.value shouldBe 0.5
    }

    "count null values as a single duplicate group" in {
      val df     = dfOf(Seq(Row(null), Row(null), Row(1)), intCol)
      val metric = UniquenessAnalyzer.analyze(df, RuleDefinition.validated(Left("id"), "is_unique"))
      metric.metadata("total_count") shouldBe 3L
      metric.metadata("duplicate_count") shouldBe 2L
      metric.value shouldBe (1.0 / 3.0)
    }

    "throw when the field is missing" in {
      val df = dfOf(Seq(Row(1)), intCol)
      an[IllegalArgumentException] should be thrownBy
      UniquenessAnalyzer.analyze(df, RuleDefinition.validated(Left("missing"), "is_unique"))
    }
  }

  "MultiFieldUniquenessAnalyzer" should {

    "count duplicate combinations of fields" in {
      val df = dfOf(
        Seq(Row(1, "a"), Row(1, "a"), Row(2, "b")),
        intCol,
        strCol
      )
      val metric = MultiFieldUniquenessAnalyzer.analyze(
        df,
        RuleDefinition.validated(Right(List("id", "code")), "are_unique")
      )
      metric.metadata("total_count") shouldBe 3L
      metric.metadata("duplicate_count") shouldBe 2L
      metric.metadata("fields") shouldBe List("id", "code")
      metric.value shouldBe (1.0 / 3.0)
    }

    "report an empty DataFrame without throwing" in {
      val empty = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(Seq(intCol, strCol))
      )
      val metric = MultiFieldUniquenessAnalyzer.analyze(
        empty,
        RuleDefinition.validated(Right(List("id", "code")), "are_unique")
      )
      metric.totalRows shouldBe 0L
      metric.metadata("duplicate_count") shouldBe 0L
      metric.value shouldBe 1.0
    }
  }
}
