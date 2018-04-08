package co.sjwiesman.flinkforward.functions.simple

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.util.Collector

object StatelessEveryNthWord {
  type StatefulFunction = (String, Option[Int]) ⇒ (TraversableOnce[String], Option[Int])
}

class StatelessEveryNthWord(n: Int) extends StatelessEveryNthWord.StatefulFunction with Serializable {
  override def apply(word: String, state: Option[Int]): (TraversableOnce[String], Option[Int]) = {
    state match {
      case Some(count) if count + 1 == n ⇒ (Some(word), None)
      case Some(count) ⇒ (None, Some(count + 1))
      case None        ⇒ (None, Some(1))
    }
  }
}

class FlatMapWithStateImpl(f: StatelessEveryNthWord.StatefulFunction) extends RichFlatMapFunction[String, String] {
  @transient private lazy val countDesc = new ValueStateDescriptor("count", classOf[Int])

  override def flatMap(value: String, out: Collector[String]): Unit = {
    val state = Option(getRuntimeContext.getState(countDesc).value())
    f(value, state) match {
      case (output, None)    ⇒
        output.foreach(out.collect)
        getRuntimeContext.getState(countDesc).clear()
      case (output, Some(x)) ⇒
        output.foreach(out.collect)
        getRuntimeContext.getState(countDesc).update(x)
    }
  }
}
