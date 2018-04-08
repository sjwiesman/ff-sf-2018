package co.sjwiesman.flinkforward.functions.complex

import java.lang.{Long => JLong}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.TimeDomain
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

object AlgebraRunner {
  class Sum extends ReduceFunction[JLong] {
    override def reduce(value1: JLong, value2: JLong): JLong = {
      value1 + value2
    }
  }
}

/**
  * Runs an implementation of the algebra
  * @param program some program to execute, all business logic is deferred to this object
  * @param few  output tag for the infrequent side output
  * @param many output tag for the heavy hitter side output
  */
class AlgebraRunner(program: Algebra, few: OutputTag[String], many: OutputTag[String]) extends ProcessFunction[String, String] {
  @transient private[this] lazy val wordDesc  = new ValueStateDescriptor("word", BasicTypeInfo.STRING_TYPE_INFO)
  @transient private[this] lazy val countDesc = new ReducingStateDescriptor("count", new AlgebraRunner.Sum, BasicTypeInfo.LONG_TYPE_INFO)
  @transient private[this] lazy val timerDesc = new ValueStateDescriptor("timer", BasicTypeInfo.LONG_TYPE_INFO)

  override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
    getRuntimeContext.getState(wordDesc).update(value)
    getRuntimeContext.getReducingState(countDesc).add(1L)

    val count = getRuntimeContext.getReducingState(countDesc).get()
    val context = Algebra.Context(count, ctx.timerService().currentWatermark())

    val Algebra.Result(output, time) = program.evaluateElem(value, context)

    output.foreach(out.collect)
    getRuntimeContext.getState(timerDesc).update(time)
    ctx.timerService().registerEventTimeTimer(time)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[String, String]#OnTimerContext, out: Collector[String]): Unit = {
    val timer = getRuntimeContext.getState(timerDesc).value()

    if (timestamp == timer && ctx.timeDomain() == TimeDomain.EVENT_TIME) {
      val count = getRuntimeContext.getReducingState(countDesc).get()
      val word  = getRuntimeContext.getState(wordDesc).value()

      program.evaluateTimer(word, count).foreach {
        case Left(a)  ⇒ ctx.output(few, a)
        case Right(a) ⇒ ctx.output(many, a)
      }

      getRuntimeContext.getReducingState(countDesc).clear()
      getRuntimeContext.getState(wordDesc).clear()
      getRuntimeContext.getState(timerDesc).clear()
    }
  }
}
