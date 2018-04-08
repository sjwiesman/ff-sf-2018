package co.sjwiesman.flinkforward.functions.complex

import org.scalatest.{FlatSpec, Matchers}

class `5_StatelessAlgebraSpec` extends FlatSpec with Matchers {
  val program = new StatelessAlgebra

  it should "not output a word when the count is less than 3" in {
    info("using this pattern we can easily test arbitrarily complex business logic")
    val context = Algebra.Context(0L, 0L)
    val result = program.evaluateElem("hello", context)
    result should be (Algebra.Result(None, 30 * 1000L))
  }

  it should "output a word when the count is 3" in {
    info("running through whatever combinations of state and data we deem interesting")
    val context = Algebra.Context(3L, 0L)
    val result = program.evaluateElem("hello", context)
    result should be (Algebra.Result(Some("hello"), 30 * 1000L))
  }

  it should "output to infrequent side output when less than 3 elements have been seen" in {
    info("even going so far as full code coverage if we want")
    val result = program.evaluateTimer("hello", 1L)
    result should be (Some(Left("hello")))
  }

  it should "output to heavy hitter side output when less than 3 elements have been seen" in {
    info("without ever once thinking about the details of Flink's runtime")

    val result = program.evaluateTimer("hello", 4L)
    result should be (Some(Right("hello")))
  }

  it should "not output to any side output when exactly 3 elements have been seen" in {
    val result = program.evaluateTimer("hello", 3L)
    result should be (None)
  }
}
