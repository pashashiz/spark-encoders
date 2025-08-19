package io.github.pashashiz.spark_encoders

class InvariantEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers {
  import InvariantEncoderSpec._

  "InvariantEncoder" when {

    "used with SimpleType" should {

      "work" in {
        new SimpleTypeWithCustomEncoder(SimpleType(true)) should haveTypedEncoder[SimpleTypeWithCustomEncoder]()
      }
    }
  }
}

object InvariantEncoderSpec {
  case class SimpleType(
    booleanField: Boolean
  )

  object SimpleType {
    val defaultValue: SimpleType = SimpleType(
      booleanField = true
    )
  }

  class SimpleTypeWithCustomEncoder(
    val toCaseClass: SimpleType
  ) {
    override def equals(obj: Any): Boolean = obj match {
      case encoder: SimpleTypeWithCustomEncoder =>
        encoder.toCaseClass == toCaseClass
      case _ =>
        false
    }
  }

  object SimpleTypeWithCustomEncoder extends Invariant[SimpleTypeWithCustomEncoder, SimpleType] {
    override def map(in: SimpleTypeWithCustomEncoder): SimpleType = in.toCaseClass

    override def contrMap(out: SimpleType): SimpleTypeWithCustomEncoder =
      new SimpleTypeWithCustomEncoder(out)

    implicit val encoder: TypedEncoder[SimpleTypeWithCustomEncoder] = InvariantEncoder(this)
  }
}

