import sbt.Keys.definedTests
import sbt.Tests
import sbt.librarymanagement.syntax._


/**
  * sorts tests in alphabetical order so that the
  * info guides are printed in chronological order
  */
object TestSort {
  private implicit val ordering: Ordering[Tests.Group] = Ordering.by(_.name.split("\\.").last)

  def sorting = {
    definedTests in Test map { tests ⇒
      val group = tests.map { test ⇒
        import Tests._
        Group(
          name = test.name,
          tests = Seq(test),
          runPolicy = InProcess)
      }.sorted
      group
    }
  }
}
