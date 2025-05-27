package com.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.util.UUID
import scala.reflect.ClassTag

case object StringEncoder extends TypedEncoder[String] {
  def catalystRepr: DataType = StringType
  def toCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = classOf[UTF8String],
      dataType = catalystRepr,
      functionName = "fromString",
      arguments = path :: Nil,
      returnNullable = false)
  def fromCatalyst(path: Expression): Expression =
    Invoke(
      targetObject = path,
      functionName = "toString",
      dataType = jvmRepr,
      returnNullable = false)
  override val toString = "StringEncoder"
}

abstract class PrimitiveEncoder[T: ClassTag](dataType: DataType) extends TypedEncoder[T] {
  override def jvmRepr = dataType
  override def catalystRepr = dataType
  def toCatalyst(path: Expression): Expression = path
  def fromCatalyst(path: Expression): Expression = path
}

case object BooleanEncoder extends PrimitiveEncoder[Boolean](BooleanType)
case object ByteEncoder extends PrimitiveEncoder[Byte](ByteType)
case object ShortEncoder extends PrimitiveEncoder[Short](ShortType)
case object IntEncoder extends PrimitiveEncoder[Int](IntegerType)
case object LongEncoder extends PrimitiveEncoder[Long](LongType)
case object FloatEncoder extends PrimitiveEncoder[Float](FloatType)
case object DoubleEncoder extends PrimitiveEncoder[Double](DoubleType)
case object BinaryEncoder extends PrimitiveEncoder[Array[Byte]](BinaryType)

case object BigDecimalEncoder extends TypedEncoder[BigDecimal] {
  override def catalystRepr: DataType = DecimalType.SYSTEM_DEFAULT
  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(Decimal.getClass, catalystRepr, "apply", path :: Nil)
  override def fromCatalyst(path: Expression): Expression =
    Invoke(path, "toBigDecimal", jvmRepr)
}

case object JBigDecimalEncoder extends TypedEncoder[JBigDecimal] {
  override def catalystRepr: DataType = DecimalType.SYSTEM_DEFAULT
  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(Decimal.getClass, catalystRepr, "apply", path :: Nil)
  override def fromCatalyst(path: Expression): Expression =
    Invoke(path, "toJavaBigDecimal", jvmRepr)
}

case object BigIntEncoder extends TypedEncoder[BigInt] {
  override def catalystRepr: DataType = DecimalType(DecimalType.MAX_PRECISION, 0)
  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(Decimal.getClass, catalystRepr, "apply", path :: Nil)
  override def fromCatalyst(path: Expression): Expression =
    Invoke(path, "toScalaBigInt", jvmRepr)
}

case object JBigIntEncoder extends TypedEncoder[JBigInt] {
  override def catalystRepr: DataType = DecimalType(DecimalType.MAX_PRECISION, 0)
  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(Decimal.getClass, catalystRepr, "apply", path :: Nil)
  override def fromCatalyst(path: Expression): Expression =
    Invoke(path, "toJavaBigInteger", jvmRepr)
}

// note: not sure that is the best idea cause could be slow
// yet having UUID as string is intuitive to be used in SQL then
case object UUIDInvariant extends Invariant[UUID, String] {
  override def map(in: UUID): String = in.toString
  override def contrMap(out: String): UUID = UUID.fromString(out)
}
