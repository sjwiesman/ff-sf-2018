package co.sjwiesman.flinkforward.functions.simple

import org.scalatest.{FlatSpec, Matchers}

class `4_StatelessEveryNthWordSpec` extends FlatSpec with Matchers {
  it should "initialize state the first time it sees a word" in {
    info("A better solution is to decouple business logic from state management.")
    info("FlatMapWithState is a stateless, referentially transparent interface")
    info("over a stateful computation.")

    val function = new StatelessEveryNthWord(3)

    val (output, state) = function("hello", None)
    output should be (Nil)
    state  should be (Some(1))
  }

  it should "modify state in the middle of a run" in {
    info("With it we can decouple our application from Flink's internals.")

    val function = new StatelessEveryNthWord(3)
    val (output, state) = function("hello", Some(1))
    output should be (Nil)
    state  should be (Some(2))
  }

  it should "modify output the word and reset state when count equals N" in {
    info("And test against different scenarios.")
    val function = new StatelessEveryNthWord(3)
    val (output, state) = function("hello", Some(2))
    output should be (List("hello"))
    state  should be (None)
  }
}
