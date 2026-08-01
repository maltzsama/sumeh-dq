package io.galileostd.sumeh.flink.config

import java.util.Optional

import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.flink.core.execution.JobClient
import org.apache.flink.table.api.{ ResultKind, TableResult }
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.types.Row
import org.apache.flink.util.CloseableIterator
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FlinkRuleLoaderSpec extends AnyWordSpec with Matchers {

  /** Minimal TableResult stub backed by a fixed list of rows. */
  private def stubResult(rows: List[Row]): TableResult = new TableResult {
    override def getJobClient(): Optional[JobClient]                             = Optional.empty()
    override def await(): Unit                                                   = ()
    override def await(timeout: Long, unit: java.util.concurrent.TimeUnit): Unit = ()
    override def getResolvedSchema(): ResolvedSchema                             = null
    override def getResultKind(): ResultKind                                     = ResultKind.SUCCESS
    override def collect(): CloseableIterator[Row] =
      new CloseableIterator[Row] {
        private val it                = rows.iterator
        override def hasNext: Boolean = it.hasNext
        override def next(): Row      = it.next()
        override def close(): Unit    = ()
      }
    override def print(): Unit = ()
  }

  private def row(values: Any*): Row = Row.of(values.map(_.asInstanceOf[AnyRef]): _*)

  "FlinkRuleLoader.fromTableResult" should {

    "require field and check_type columns" in {
      val result = stubResult(Nil)
      an[IllegalArgumentException] should be thrownBy FlinkRuleLoader.fromTableResult(result, Array("field"))
    }

    "load rules from rows" in {
      val result = stubResult(List(row("email", "is_complete", null), row("age", "is_greater_than", "18")))
      val rules  = FlinkRuleLoader.fromTableResult(result, Array("field", "check_type", "value"))
      rules should have size 2
      rules.head.checkType shouldBe "is_complete"
      rules.head.field shouldBe Left("email")
      rules(1).value shouldBe defined
    }

    "carry metadata columns" in {
      val result = stubResult(List(row("age", "is_positive", "0", "ROW", "comparison")))
      val rule =
        FlinkRuleLoader.fromTableResult(result, Array("field", "check_type", "execute", "level", "category")).head
      rule.execute shouldBe false
      rule.level shouldBe "ROW"
      rule.category shouldBe "comparison"
    }

    "treat null cells as empty strings" in {
      val result = stubResult(List(row("email", "is_complete", null, null)))
      val rule   = FlinkRuleLoader.fromTableResult(result, Array("field", "check_type", "execute", "value")).head
      rule.value shouldBe None
      rule.execute shouldBe true
    }

    "support multi-field bracket notation" in {
      val result = stubResult(List(row("[id, name]", "are_complete")))
      val rule   = FlinkRuleLoader.fromTableResult(result, Array("field", "check_type")).head
      rule.field shouldBe Right(List("id", "name"))
    }
  }
}
