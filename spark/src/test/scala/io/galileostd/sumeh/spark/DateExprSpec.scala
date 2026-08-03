package io.galileostd.sumeh.spark

import org.apache.spark.sql.{ functions => F, SparkSession }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class DateExprSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-dateexpr")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private def eval(input: String): Option[java.sql.Date] = {
    val session = spark
    import session.implicits._
    val df = Seq(input).toDF("c").select(DateExpr.safeToDate(F.col("c")).alias("d"))
    Option(df.collect()(0).getAs[java.sql.Date](0))
  }

  "safeToDate" should {

    "parse an ISO date" in {
      eval("2026-08-02") shouldBe defined
    }

    "parse a SQL timestamp string" in {
      eval("2026-08-02 10:30:00") shouldBe defined
    }

    "return null (not throw) on garbage" in {
      eval("not-a-date") shouldBe None
    }

    "return null on empty string" in {
      eval("") shouldBe None
    }

    "not throw under ANSI mode" in {
      val prev = spark.conf.getOption("spark.sql.ansi.enabled")
      spark.conf.set("spark.sql.ansi.enabled", "true")
      try noException should be thrownBy eval("31/12/2026")
      finally
        prev match {
          case Some(v) => spark.conf.set("spark.sql.ansi.enabled", v)
          case None    => spark.conf.unset("spark.sql.ansi.enabled")
        }
    }
  }
}
