package co.sjwiesman.flinkforward.source

import java.util

import co.sjwiesman.flinkforward.util.MockSourceContext
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class `1_ResourceToStreamSpec` extends FlatSpec with Matchers with MockitoSugar {
  it should "process a resource file" in {
    info("Operator state provides a clean separation of concerns between")
    info("business logic and state management. This means we can first test")
    info("the business logic of our application in isolation.")

    val runner = new ResourceToStream("/testfile.txt")
    val ctx    = new MockSourceContext[String]

    runner.run(ctx)

    ctx.output.size should be (4)
    ctx.output should be (ListBuffer("hello", "world", "good", "day"))
  }

  it should "restore from state" in {
    info("Once the business logic has been validated, we can turn our")
    info("attention to state management. In this particular case,")
    info("ListCheckpointed provides a high level interface, meaning it")
    info("is simple to simulate recovering from a failure.")

    val runner = new ResourceToStream("/testfile.txt")
    val ctx    = new MockSourceContext[String]

    runner.restoreState(util.Arrays.asList(2))
    runner.run(ctx)

    ctx.output should be (ListBuffer("good", "day"))

    ctx.output.size should be (2)
  }
}
