package co.sjwiesman.flinkforward.functions.simple

object StatelessCount {
  /**
    * The method signature for [[org.apache.flink.streaming.api.scala.DataStream#flatMapWithState]]
    * specialized for performing every n'th word computations.
    *
    * By making the state an explicit parameter and return element from the method, flatMapWithState
    * is a very simple function signature to reason about and test.
    */
  type StatefulFunction = (String, Option[Int]) ⇒ ((String, Int), Option[Int])
}

class StatelessCount extends StatelessCount.StatefulFunction with Serializable {
  override def apply(word: String, state: Option[Int]): ((String, Int), Option[Int]) = state match {
    case Some(count) ⇒ ((word, count + 1), Some(count + 1))
    case None        ⇒ ((word, 1), Some(1))
  }
}
