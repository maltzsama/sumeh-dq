package io.galileostd.sumeh.flink

import scala.collection.JavaConverters._

import io.galileostd.sumeh.rule.{ RuleDefinition, StringValue }
import org.apache.flink.api.common.typeinfo.{ TypeInformation, Types }
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.types.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FlinkValidatorSpec extends AnyWordSpec with Matchers {

  private val rowType = new RowTypeInfo(
    Array[TypeInformation[_]](Types.INT, Types.STRING, Types.INT),
    Array[String]("id", "name", "age")
  )

  private def positionalRow(id: Int, name: String, age: Int): Row = {
    val r = new Row(3)
    r.setField(0, Int.box(id))
    r.setField(1, name)
    r.setField(2, Int.box(age))
    r
  }

  private def streamOf(rows: Row*): DataStream[Row] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromCollection(rows.toList.asJava, rowType)
  }

  "FlinkValidator" should {

    "bifurcate a stream into good and bad side outputs" in {
      val validated = FlinkValidator.validate(
        streamOf(positionalRow(1, "alice", 30), positionalRow(2, null, 25), positionalRow(3, "bob", -5)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )

      val (good, bad) = validated.split()
      val goodRows    = good.executeAndCollect(10)
      val badRows     = bad.executeAndCollect(10)

      goodRows.asScala.map(_.getField(0).asInstanceOf[Int]).toSet shouldBe Set(1)
      badRows.asScala.map(_.getField(0).asInstanceOf[Int]).toSet shouldBe Set(2, 3)
    }

    "annotate _dq_errors and _dq_skipped on every row" in {
      val validated = FlinkValidator.validate(
        streamOf(positionalRow(1, "alice", 30), positionalRow(2, null, 25)),
        Seq(
          RuleDefinition.validated(Left("id"), "is_unique"),
          RuleDefinition.validated(Left("name"), "is_complete")
        )
      )

      val rows = validated.toNative.executeAndCollect(10).asScala.toList

      rows.size shouldBe 2
      rows.foreach {
        r =>
          r.getArity shouldBe 5
          r.getField(4).toString should include("is_unique")
      }
    }

    "send every row to the bad stream when all rules fail" in {
      val validated = FlinkValidator.validate(
        streamOf(positionalRow(1, null, -5), positionalRow(2, null, -1)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )

      val (good, bad) = validated.split()
      good.executeAndCollect(10) shouldBe empty
      bad.executeAndCollect(10).asScala.map(_.getField(0).asInstanceOf[Int]).toSet shouldBe Set(1, 2)
    }

    "annotate _dq_skipped for unsupported rules on every row" in {
      val validated = FlinkValidator.validate(
        streamOf(positionalRow(1, "alice", 30), positionalRow(2, "bob", 25)),
        Seq(
          RuleDefinition.validated(Left("id"), "is_unique"),
          RuleDefinition.validated(Left("age"), "has_mean"),
          RuleDefinition.validated(Left("name"), "satisfies", value = Some(StringValue("name = 'x'")))
        )
      )

      val (good, bad) = validated.split()
      val goodRows    = good.executeAndCollect(10).asScala.toList
      val badRows     = bad.executeAndCollect(10).asScala.toList

      goodRows.size shouldBe 2
      badRows shouldBe empty
      goodRows.foreach {
        r =>
          r.getField(4).toString should include("is_unique")
          r.getField(4).toString should include("has_mean")
          r.getField(4).toString should include("satisfies")
      }
    }

    "handle an empty stream without failing" in {
      val validated = FlinkValidator.validate(
        streamOf(),
        Seq(RuleDefinition.validated(Left("name"), "is_complete"))
      )
      validated.toNative.executeAndCollect(10) shouldBe empty
    }

    "process positional rows coming from a source with RowTypeInfo" in {
      val validated = FlinkValidator.validate(
        streamOf(positionalRow(1, "alice", 30), positionalRow(2, null, 25), positionalRow(3, "bob", -5)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )

      val rows = validated.toNative.executeAndCollect(10).asScala.toList
      rows.size shouldBe 3

      val (good, bad) = validated.split()
      good.executeAndCollect(10).asScala.map(_.getField(0).asInstanceOf[Int]).toSet shouldBe Set(1)
      bad.executeAndCollect(10).asScala.map(_.getField(0).asInstanceOf[Int]).toSet shouldBe Set(2, 3)
    }

    "declare a RowTypeInfo on the output, not a GenericTypeInfo" in {
      val validated = FlinkValidator.validate(
        streamOf(positionalRow(1, "alice", 30)),
        Seq(RuleDefinition.validated(Left("name"), "is_complete"))
      )
      validated.toNative.getType shouldBe a[RowTypeInfo]
      validated.toNative.getType.asInstanceOf[RowTypeInfo].getFieldNames should contain("_dq_errors")
      validated.toNative.getType.asInstanceOf[RowTypeInfo].getFieldNames should contain("_dq_skipped")
    }

    "throw a clear message when the stream has no RowTypeInfo" in {
      import org.apache.flink.api.common.typeinfo.Types
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      val untyped = env
        .fromCollection(java.util.Collections.singletonList(new Row(0)))
        .returns(Types.GENERIC(classOf[Row]))

      an[IllegalArgumentException] should be thrownBy FlinkValidator.validate(untyped, Seq.empty)
    }

    "emit each row exactly once on the main output" in {
      val validated = FlinkValidator.validate(
        streamOf(positionalRow(1, "alice", 30), positionalRow(2, null, 25), positionalRow(3, "bob", -5)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )

      validated.toNative.executeAndCollect(10).asScala should have size 3
    }
  }
}
