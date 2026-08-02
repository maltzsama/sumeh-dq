package io.galileostd.sumeh

import io.galileostd.sumeh.spark.SparkProfiler
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class SparkProfilerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-profiler")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  def dfMixed = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row(1, "alice", 30.0, "active"),
        Row(2, "bob", 25.5, "active"),
        Row(3, null, 40.0, "inactive"),
        Row(4, "diana", 17.0, null)
      )
    ),
    StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("age", DoubleType, nullable = true),
        StructField("status", StringType, nullable = true)
      )
    )
  )

  "SparkProfiler" should {

    "report table-level stats" in {
      val profile = SparkProfiler.profile(dfMixed)
      profile.tableStats("total_rows") shouldBe 4L
      profile.tableStats("columns_count") shouldBe 4L
    }

    "measure completeness and cardinality per column" in {
      val profile = SparkProfiler.profile(dfMixed)
      val name    = profile.columnProfiles("name")
      name.completeness shouldBe 0.75
      name.distinctCount shouldBe 3L
      name.nullCount shouldBe 1L
      name.uniqueness shouldBe 0.75

      profile.columnProfiles("status").distinctCount shouldBe 2L
    }

    "measure numeric stats only for numeric columns" in {
      val profile = SparkProfiler.profile(dfMixed)
      val age     = profile.columnProfiles("age")
      age.min.get shouldBe 17.0
      age.max.get shouldBe 40.0
      age.mean.get shouldBe 28.125
      age.sum.get shouldBe 112.5
      age.stdDev.get should be > 0.0

      profile.columnProfiles("name").min shouldBe None
      profile.columnProfiles("name").sum shouldBe None
    }

    "report correct types and nullability" in {
      val profile = SparkProfiler.profile(dfMixed)
      profile.columnProfiles("age").`type` shouldBe "double"
      profile.columnProfiles("age").nullable shouldBe true
    }

    "support fractional sampling" in {
      val big = spark.createDataFrame(
        spark.sparkContext.parallelize((1 to 100).map(i => Row(i, s"v$i"))),
        StructType(
          Seq(StructField("id", IntegerType, nullable = true), StructField("name", StringType, nullable = true))
        )
      )
      val profile = SparkProfiler.profile(big, sampleFraction = Some(0.5))
      val rows    = profile.tableStats("total_rows").asInstanceOf[Long]
      rows should be > 0L
      rows should be <= 100L
    }

    "serialize to JSON" in {
      val json = SparkProfiler.profile(dfMixed).toJson
      json should include("table_stats")
      json should include("column_profiles")
      json should include("total_rows")
      val parsed = ujson.read(json)
      parsed("table_stats")("total_rows").num shouldBe 4.0
      parsed("column_profiles")("age")("mean").num shouldBe 28.125
    }

    "profile a table in a constant number of Spark jobs" in {
      import org.apache.spark.scheduler.{ SparkListener, SparkListenerJobStart }

      class JobCounter extends SparkListener {
        @volatile var jobs: Int                                        = 0
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = jobs += 1
      }

      def table(colCount: Int): org.apache.spark.sql.DataFrame = {
        val rows = (1 to 20).map {
          i =>
            val values = (0 until colCount).flatMap(_ => Seq(Integer.valueOf(i), java.lang.Double.valueOf(i.toDouble)))
            Row.fromSeq(values.toList)
        }
        val fields = (0 until colCount).flatMap {
          i =>
            Seq(
              StructField(s"c${i}_int", IntegerType, nullable = true),
              StructField(s"c${i}_dbl", DoubleType, nullable = true)
            )
        }
        spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(fields))
      }

      val counter = new JobCounter
      spark.sparkContext.addSparkListener(counter)
      try {
        SparkProfiler.profile(table(2)) // warm-up
        counter.jobs = 0
        SparkProfiler.profile(table(2))
        val jobsSmall = counter.jobs

        counter.jobs = 0
        SparkProfiler.profile(table(10))
        val jobsLarge = counter.jobs

        jobsSmall shouldBe jobsLarge
      } finally
        spark.sparkContext.removeSparkListener(counter)
    }

    "return None for stats of an empty column" in {
      val empty = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(Seq(StructField("n", DoubleType, nullable = true)))
      )
      val profile = SparkProfiler.profile(empty)
      val col     = profile.columnProfiles("n")
      col.min shouldBe None
      col.max shouldBe None
      col.mean shouldBe None
      col.stdDev shouldBe None
      col.sum shouldBe None
      col.completeness shouldBe 1.0
    }
  }
}
