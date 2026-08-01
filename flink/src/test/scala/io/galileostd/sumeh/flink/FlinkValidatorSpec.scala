package io.galileostd.sumeh.flink

import scala.collection.JavaConverters._

import io.galileostd.sumeh.rule.RuleDefinition
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

  private def namedRow(id: Int, name: String, age: Int): Row = {
    val r = Row.withNames()
    r.setField("id", Int.box(id))
    r.setField("name", name)
    r.setField("age", Int.box(age))
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
        streamOf(namedRow(1, "alice", 30), namedRow(2, null, 25), namedRow(3, "bob", -5)),
        Seq(
          RuleDefinition.validated(Left("name"), "is_complete"),
          RuleDefinition.validated(Left("age"), "is_positive")
        )
      )

      val (good, bad) = validated.split()
      val goodRows    = good.executeAndCollect(10)
      val badRows     = bad.executeAndCollect(10)

      goodRows.asScala.map(_.getField("id").asInstanceOf[Int]).toSet shouldBe Set(1)
      badRows.asScala.map(_.getField("id").asInstanceOf[Int]).toSet shouldBe Set(2, 3)
    }

    "annotate _dq_errors and _dq_skipped on every row" in {
      val validated = FlinkValidator.validate(
        streamOf(namedRow(1, "alice", 30), namedRow(2, null, 25)),
        Seq(
          RuleDefinition.validated(Left("id"), "is_unique"),
          RuleDefinition.validated(Left("name"), "is_complete")
        )
      )

      val rows = validated.toNative.executeAndCollect(10).asScala.toList

      rows.size shouldBe 2
      rows.foreach {
        r =>
          r.getFieldNames(true).asScala.toSet shouldBe Set("id", "name", "age", "_dq_errors", "_dq_skipped")
          r.getField("_dq_skipped").toString should include("is_unique")
      }
    }
  }
}
