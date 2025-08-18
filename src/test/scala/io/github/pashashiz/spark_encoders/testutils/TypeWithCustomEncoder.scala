package io.github.pashashiz.spark_encoders.testutils

import io.github.pashashiz.spark_encoders.Invariant


sealed trait TypeWithCustomEncoder

case class TypeWithCustomEncoderA(
  booleanField: Boolean
) extends TypeWithCustomEncoder

case class TypeWithCustomEncoderB(
  intField: Int
) extends TypeWithCustomEncoder

case object TypeWithCustomEncoderC extends TypeWithCustomEncoder

object TypeWithCustomEncoderA {
  val defaultValue: TypeWithCustomEncoderA = TypeWithCustomEncoderA(
    booleanField = true
  )
}

object TypeWithCustomEncoderB {
  val defaultValue: TypeWithCustomEncoderB = TypeWithCustomEncoderB(
    intField = 42
  )
}

object TypeWithCustomEncoder {
  val defaultValue: TypeWithCustomEncoder = TypeWithCustomEncoderA.defaultValue
}

class SimpleTypeWithCustomEncoder(
  val toCaseClass: TypeWithCustomEncoder
) extends Invariant[SimpleTypeWithCustomEncoder, TypeWithCustomEncoder] {
  override def map(in: SimpleTypeWithCustomEncoder): TypeWithCustomEncoder = in.toCaseClass

  override def contrMap(out: TypeWithCustomEncoder): SimpleTypeWithCustomEncoder =
    new SimpleTypeWithCustomEncoder(out)
}
