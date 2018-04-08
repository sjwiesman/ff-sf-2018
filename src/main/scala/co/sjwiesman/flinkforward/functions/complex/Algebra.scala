package co.sjwiesman.flinkforward.functions.complex

object Algebra {

  /**
    * The current state of the system
    * @param count number of elements seen so far
    * @param watermark current watermark
    */
  final case class Context(count: Long, watermark: Long)

  /**
    * The output result set
    * @param word an optional word to output to downstream operators
    * @param timer the next event time timer to set. note: this overrides existing timers
    */
  final case class Result(word: Option[String], timer: Long)
}

trait Algebra {
  /**
    * evaluated when a word in
    * @param word the word that this session is keyed on
    * @param ctx the current state of the system
    * @return the resulting output
    */
  def evaluateElem(word: String, ctx: Algebra.Context): Algebra.Result

  /**
    * evaluated when the final timer fires, just before all state is cleared
    * @param word the word that this session is keyed on
    * @param count the final count of the window
    * @return optionally a [[Left]] word to go in the infrequent stream or a [[Right]] to go in the heavy hitter stream
    */
  def evaluateTimer(word: String, count: Long): Option[Either[String, String]]
}
