package io.github.pashashiz.spark_encoders.testutils

import io.github.pashashiz.spark_encoders.{Invariant, InvariantEncoder, TypedEncoder}


sealed trait SimpleType

case class SimpleTypeA(
  booleanField: Boolean
) extends SimpleType

case class SimpleTypeB(
  intField: Int
) extends SimpleType

case object SimpleTypeC extends SimpleType

object SimpleTypeA {
  val defaultValue: SimpleTypeA = SimpleTypeA(
    booleanField = true
  )
}

object SimpleTypeB {
  val defaultValue: SimpleTypeB = SimpleTypeB(
    intField = 42
  )
}

object SimpleType {
  val defaultValue: SimpleType = SimpleTypeA.defaultValue
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
