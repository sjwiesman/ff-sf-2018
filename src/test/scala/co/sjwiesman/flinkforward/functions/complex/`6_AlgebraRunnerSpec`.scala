package co.sjwiesman.flinkforward.functions.complex

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.api.scala._

class `6_AlgebraRunnerSpec` extends FlatSpec with Matchers {
  private[this] val heavyHitter = new OutputTag[String]("heavy-hitter")
  private[this] val infrequent  = new OutputTag[String]("infrequent")
  private[this] val keySelector = new KeySelector[String, String] {
    override def getKey(value: String): String = value
  }

  it should "output a word and set a timer" in {
    info("similarly the the EveyNthWord example, these test's are more fragile")

    val program = new Algebra {
      override def evaluateTimer(word: String, count: Long) = None
      override def evaluateElem(word: String, ctx: Algebra.Context) = {
        Algebra.Result(Some(word), 10L)
      }
    }

    val runner = new AlgebraRunner(program, infrequent, heavyHitter)
    val operator = new KeyedProcessOperator(runner)
    val testHarness = new KeyedOneInputStreamOperatorTestHarness(
      operator,
      keySelector,
      BasicTypeInfo.STRING_TYPE_INFO
    )

    testHarness.open()
    testHarness.processElement("hello", 0L)

    testHarness.getOutput.size() should be (1)
    testHarness.numEventTimeTimers() should be (1)

    testHarness.close()
  }

  it should "output to the infrequent side output" in {
    info("however, there are very few of them, testing very simple code paths")
    val program = new Algebra {
      override def evaluateTimer(word: String, count: Long) = {
        Some(Left(word))
      }
      override def evaluateElem(word: String, ctx: Algebra.Context) = {
        Algebra.Result(Some(word), 10L)
      }
    }

    val runner = new AlgebraRunner(program, infrequent, heavyHitter)
    val operator = new KeyedProcessOperator(runner)
    val testHarness = new KeyedOneInputStreamOperatorTestHarness(
      operator,
      keySelector,
      BasicTypeInfo.STRING_TYPE_INFO
    )

    testHarness.open()
    testHarness.processElement("hello", 0L)

    testHarness.processWatermark(11L)
    testHarness.getSideOutput(infrequent).size() should be (1)

    testHarness.close()
  }

  it should "output to the heavy hitter side output" in {
    info("making them simple to maintain and understand")

    val program = new Algebra {
      override def evaluateTimer(word: String, count: Long) = {
        Some(Right(word))
      }
      override def evaluateElem(word: String, ctx: Algebra.Context) = {
        Algebra.Result(Some(word), 10L)
      }
    }

    val runner = new AlgebraRunner(program, infrequent, heavyHitter)
    val operator = new KeyedProcessOperator(runner)
    val testHarness = new KeyedOneInputStreamOperatorTestHarness(
      operator,
      keySelector,
      BasicTypeInfo.STRING_TYPE_INFO
    )

    testHarness.open()
    testHarness.processElement("hello", 0L)

    testHarness.processWatermark(11L)
    testHarness.getSideOutput(heavyHitter).size() should be (1)

    testHarness.close()
  }
}
