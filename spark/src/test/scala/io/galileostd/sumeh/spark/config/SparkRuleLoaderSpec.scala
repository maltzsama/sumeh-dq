package io.galileostd.sumeh.spark.config

import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class SparkRuleLoaderSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-rules")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private def rulesDf(rows: Seq[Row], schema: StructType) =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  private val schema = StructType(
    Seq(
      StructField("field", StringType, nullable = true),
      StructField("check_type", StringType, nullable = true),
      StructField("value", StringType, nullable = true),
      StructField("threshold", StringType, nullable = true),
      StructField("execute", StringType, nullable = true),
      StructField("level", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("env", StringType, nullable = true)
    )
  )

  "SparkRuleLoader.fromDataFrame" should {

    "load rules from a dataframe" in {
      val df = rulesDf(
        Seq(
          Row("email", "is_complete", null, null, null, null, null, null),
          Row("age", "is_greater_than", "18", null, "0", "ROW", "comparison", "prod")
        ),
        schema
      )
      val rules = SparkRuleLoader.fromDataFrame(df)
      rules should have size 2
      rules.head.checkType shouldBe "is_complete"
      rules.head.field shouldBe Left("email")
    }

    "carry value, execute and metadata" in {
      val df = rulesDf(
        Seq(Row("age", "is_greater_than", "18", null, "0", "ROW", "comparison", "prod")),
        schema
      )
      val rule = SparkRuleLoader.fromDataFrame(df).head
      rule.value shouldBe defined
      rule.execute shouldBe false
      rule.level shouldBe "ROW"
      rule.category shouldBe "comparison"
      rule.metadata("env") shouldBe "prod"
    }

    "treat null cells as empty strings" in {
      val df = rulesDf(
        Seq(Row("email", "is_complete", null, null, null, null, null, null)),
        schema
      )
      val rule = SparkRuleLoader.fromDataFrame(df).head
      rule.value shouldBe None
      rule.execute shouldBe true
    }

    "throw when required columns are missing" in {
      val df = rulesDf(Seq(Row("email")), StructType(Seq(StructField("field", StringType))))
      an[IllegalArgumentException] should be thrownBy SparkRuleLoader.fromDataFrame(df)
    }

    "reject an oversized rule source" in {
      val rows = (1 to 10001).map(i => Row(s"col$i", "is_complete"))
      val df = rulesDf(
        rows,
        StructType(
          Seq(
            StructField("field", StringType, nullable = true),
            StructField("check_type", StringType, nullable = true)
          )
        )
      )
      an[IllegalArgumentException] should be thrownBy SparkRuleLoader.fromDataFrame(df)
    }

    "accept a rule source exactly at the limit" in {
      val rows = (1 to 10000).map(i => Row(s"col$i", "is_complete"))
      val df = rulesDf(
        rows,
        StructType(
          Seq(
            StructField("field", StringType, nullable = true),
            StructField("check_type", StringType, nullable = true)
          )
        )
      )
      SparkRuleLoader.fromDataFrame(df) should have size 10000
    }
  }

  "SparkRuleLoader.fromJsonColumn" should {

    "require the json column to exist" in {
      val df = rulesDf(Seq(Row("x")), StructType(Seq(StructField("other", StringType))))
      an[IllegalArgumentException] should be thrownBy SparkRuleLoader.fromJsonColumn(df, "config")
    }

    "parse rules out of a json column" in {
      val json  = """{"field": "email", "check_type": "is_complete"}"""
      val df    = rulesDf(Seq(Row(json)), StructType(Seq(StructField("config", StringType))))
      val rules = SparkRuleLoader.fromJsonColumn(df, "config")
      rules should have size 1
      rules.head.checkType shouldBe "is_complete"
    }

    "parse multiple rules from a single json array" in {
      val json =
        """[{"field": "email", "check_type": "is_complete"}, {"field": "age", "check_type": "is_positive"}]"""
      val df    = rulesDf(Seq(Row(json)), StructType(Seq(StructField("config", StringType))))
      val rules = SparkRuleLoader.fromJsonColumn(df, "config")
      rules should have size 2
      rules.map(_.checkType) shouldBe List("is_complete", "is_positive")
    }

    "accumulate rules across multiple rows" in {
      val df = rulesDf(
        Seq(
          Row("""{"field": "a", "check_type": "is_complete"}"""),
          Row("""{"field": "b", "check_type": "is_complete"}""")
        ),
        StructType(Seq(StructField("config", StringType)))
      )
      SparkRuleLoader.fromJsonColumn(df, "config") should have size 2
    }

    "skip empty or invalid json" in {
      val df = rulesDf(
        Seq(Row(""), Row("not-json"), Row("""{"field": "a", "check_type": "is_complete"}""")),
        StructType(Seq(StructField("config", StringType)))
      )
      val rules = SparkRuleLoader.fromJsonColumn(df, "config")
      rules should have size 1
    }

    "reject an oversized json column" in {
      val rows = (1 to 10001).map(_ => Row("""{"field": "a", "check_type": "is_complete"}"""))
      val df   = rulesDf(rows, StructType(Seq(StructField("config", StringType))))
      an[IllegalArgumentException] should be thrownBy SparkRuleLoader.fromJsonColumn(df, "config")
    }
  }
}
