package co.sjwiesman.flinkforward.functions.complex


class StatelessAlgebra extends Algebra {
  final private[this] val thrity_seconds = 30 * 1000L

  override def evaluateElem(word: String, ctx: Algebra.Context): Algebra.Result = {
    val output = if (ctx.count == 3) {
      Some(word)
    } else {
      None
    }

    val timer = ctx.watermark + thrity_seconds

    Algebra.Result(output, timer)
  }

  override def evaluateTimer(word: String, count: Long): Option[Either[String, String]] = {
    if (count < 3) {
      Some(Left(word))
    } else if (count > 3) {
      Some(Right(word))
    } else {
      None
    }
  }
}
