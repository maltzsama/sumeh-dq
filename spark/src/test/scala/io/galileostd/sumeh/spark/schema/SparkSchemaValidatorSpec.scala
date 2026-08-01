package io.galileostd.sumeh.spark.schema

import io.galileostd.sumeh.schema.{ ColumnDef, SchemaDef }
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

class SparkSchemaValidatorSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("sumeh-test-schema")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private def df(schema: StructType): org.apache.spark.sql.DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row.fromSeq(Seq.empty))), schema)

  private def col(name: String, props: Any): ColumnDef = ColumnDef.fromMap(name, props)

  "SparkSchemaValidator.extractSchema" should {

    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("price", DecimalType(10, 2), nullable = true),
        StructField("when", TimestampType, nullable = true),
        StructField("tags", ArrayType(StringType), nullable = true),
        StructField(
          "friends",
          ArrayType(StructType(Seq(StructField("nickname", StringType), StructField("since", IntegerType)))),
          nullable = true
        ),
        StructField(
          "address",
          StructType(Seq(StructField("street", StringType), StructField("zip", IntegerType))),
          nullable = true
        )
      )
    )

    "map scalar types to canonical types" in {
      val extracted = SparkSchemaValidator.extractSchema(df(schema))
      extracted("id")("raw_type") shouldBe "long"
      extracted("name")("raw_type") shouldBe "string"
      extracted("price")("raw_type").toString should startWith("decimal")
      extracted("when")("raw_type") shouldBe "timestamp"
    }

    "record nullability" in {
      val extracted = SparkSchemaValidator.extractSchema(df(schema))
      extracted("id")("nullable") shouldBe true
    }

    "record element type for plain arrays" in {
      val extracted = SparkSchemaValidator.extractSchema(df(schema))
      extracted("tags")("element_type") shouldBe "string"
    }

    "extract nested fields for array-of-struct" in {
      val extracted = SparkSchemaValidator.extractSchema(df(schema))
      val nested    = extracted("friends")("nested_fields").asInstanceOf[Map[String, Map[String, Any]]]
      nested("nickname")("raw_type") shouldBe "string"
      nested("since")("raw_type") shouldBe "integer"
    }

    "extract nested fields for struct columns" in {
      val extracted = SparkSchemaValidator.extractSchema(df(schema))
      val nested    = extracted("address")("nested_fields").asInstanceOf[Map[String, Map[String, Any]]]
      nested.keySet shouldBe Set("street", "zip")
    }
  }

  "SparkSchemaValidator.validate" should {

    val goodSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = true)
      )
    )

    val expected = SchemaDef(
      columns = List(
        ColumnDef("id", "integer", nullable = false),
        ColumnDef("name", "string")
      )
    )

    "pass a conforming schema" in {
      val report = SparkSchemaValidator.validate(df(goodSchema), expected)
      report.passed shouldBe true
      report.totalIssues shouldBe 0
    }

    "flag a missing required column" in {
      val onlyId = StructType(Seq(StructField("id", LongType)))
      val report = SparkSchemaValidator.validate(df(onlyId), expected)
      report.passed shouldBe false
      report.missingCols shouldBe List("name")
    }

    "ignore optional missing columns" in {
      val withOptional = expected.copy(columns = expected.columns :+ ColumnDef("extra", "string", isOptional = true))
      val onlyId       = StructType(Seq(StructField("id", LongType)))
      val report       = SparkSchemaValidator.validate(df(onlyId), withOptional)
      report.passed shouldBe false
      report.missingCols shouldBe List("name")
    }

    "flag a type mismatch" in {
      val wrongType = StructType(
        Seq(StructField("id", LongType, nullable = false), StructField("name", IntegerType, nullable = true))
      )
      val report = SparkSchemaValidator.validate(df(wrongType), expected)
      report.passed shouldBe false
      (report.typeErrors should contain).key("name")
    }

    "accept integer types for a long column" in {
      val report = SparkSchemaValidator.validate(df(goodSchema), expected)
      report.typeErrors should not contain key("id")
    }

    "flag a nullable column that must be NOT NULL" in {
      val nullableId = StructType(
        Seq(StructField("id", LongType, nullable = true), StructField("name", StringType, nullable = true))
      )
      val report = SparkSchemaValidator.validate(df(nullableId), expected)
      report.passed shouldBe false
      (report.typeErrors should contain).key("id")
    }

    "flag extra columns when strict" in {
      val schema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType),
          StructField("surprise", StringType)
        )
      )
      val strict = SchemaDef(expected.columns, strictColumns = true)
      val report = SparkSchemaValidator.validate(df(schema), strict)
      report.passed shouldBe false
      report.extraCols shouldBe List("surprise")
    }

    "allow extra columns when not strict" in {
      val schema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType),
          StructField("surprise", StringType)
        )
      )
      val report = SparkSchemaValidator.validate(df(schema), expected)
      report.passed shouldBe true
    }

    "flag missing required comment" in {
      val withComment = SchemaDef(
        List(
          ColumnDef("id", "integer", nullable = false, requireComment = true),
          ColumnDef("name", "string")
        )
      )
      val report = SparkSchemaValidator.validate(df(goodSchema), withComment)
      report.passed shouldBe false
      (report.metadataErrors should contain).key("id")
    }

    "pass when a comment is present" in {
      val commented = StructType(
        Seq(
          StructField(
            "id",
            LongType,
            nullable = false,
            new MetadataBuilder().putString("comment", "primary key").build()
          ),
          StructField("name", StringType)
        )
      )
      val withComment = SchemaDef(
        List(
          ColumnDef("id", "integer", nullable = false, requireComment = true),
          ColumnDef("name", "string")
        )
      )
      val report = SparkSchemaValidator.validate(df(commented), withComment)
      report.passed shouldBe true
    }

    "flag expected comment mismatch" in {
      val commented = StructType(
        Seq(
          StructField(
            "id",
            LongType,
            nullable = false,
            new MetadataBuilder().putString("comment", "something else").build()
          ),
          StructField("name", StringType)
        )
      )
      val withExpected = SchemaDef(
        List(
          ColumnDef("id", "integer", nullable = false, expectedComment = Some("primary key")),
          ColumnDef("name", "string")
        )
      )
      val report = SparkSchemaValidator.validate(df(commented), withExpected)
      report.passed shouldBe false
      (report.metadataErrors should contain).key("id")
    }

    "flag array element type mismatch" in {
      val arraySchema = StructType(Seq(StructField("tags", ArrayType(StringType))))
      val defn        = SchemaDef(List(col("tags", Map("type" -> "array", "element_type" -> "integer"))))
      val report      = SparkSchemaValidator.validate(df(arraySchema), defn)
      report.passed shouldBe false
      (report.typeErrors should contain).key("tags")
    }

    "pass matching array element types" in {
      val arraySchema = StructType(Seq(StructField("tags", ArrayType(StringType))))
      val defn        = SchemaDef(List(col("tags", Map("type" -> "array", "element_type" -> "string"))))
      val report      = SparkSchemaValidator.validate(df(arraySchema), defn)
      report.passed shouldBe true
    }

    "validate nested struct fields" in {
      val nestedSchema = StructType(
        Seq(StructField("address", StructType(Seq(StructField("street", StringType), StructField("zip", IntegerType)))))
      )
      val defn = SchemaDef(
        List(
          col(
            "address",
            Map(
              "type"   -> "struct",
              "fields" -> Map("street" -> "string", "zip" -> "integer")
            )
          )
        )
      )
      val report = SparkSchemaValidator.validate(df(nestedSchema), defn)
      report.passed shouldBe true
    }

    "flag nested struct type errors" in {
      val nestedSchema = StructType(
        Seq(StructField("address", StructType(Seq(StructField("street", StringType), StructField("zip", StringType)))))
      )
      val defn = SchemaDef(
        List(
          col(
            "address",
            Map(
              "type"   -> "struct",
              "fields" -> Map("street" -> "string", "zip" -> "integer")
            )
          )
        )
      )
      val report = SparkSchemaValidator.validate(df(nestedSchema), defn)
      report.passed shouldBe false
      (report.typeErrors should contain).key("address.zip")
    }

    "treat date and timestamp as datetime" in {
      val dateSchema = StructType(Seq(StructField("when", DateType)))
      val defn       = SchemaDef(List(col("when", "datetime")))
      SparkSchemaValidator.validate(df(dateSchema), defn).passed shouldBe true
    }
  }
}
