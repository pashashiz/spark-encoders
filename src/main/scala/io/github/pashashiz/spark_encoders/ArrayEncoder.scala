package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.compatibility.staticInvoke
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, Invoke, MapObjects}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

object ArrayEncoder {
  def apply[A](implicit
      elementClassTag: ClassTag[A],
      elementEncoder: TypedEncoder[A]): TypedEncoder[Array[A]] =
    elementEncoder.jvmRepr match {
      case ByteType =>
        BinaryEncoder.asInstanceOf[TypedEncoder[Array[A]]]
      case dataType if Primitive.isPrimitive(dataType) =>
        PrimitiveArrayEncoder()
      case _ =>
        ObjectArrayEncoder()
    }
}

case class PrimitiveArrayEncoder[A]()(implicit
    elementClassTag: ClassTag[A],
    elementEncoder: TypedEncoder[A])
    extends TypedEncoder[Array[A]] {

  override def catalystRepr: DataType =
    ArrayType(elementEncoder.catalystRepr, elementEncoder.nullable)

  override def toCatalyst(path: Expression): Expression =
    staticInvoke(
      staticObject = classOf[UnsafeArrayData],
      dataType = catalystRepr,
      functionName = "fromPrimitiveArray",
      arguments = path :: Nil)

  override def fromCatalyst(path: Expression): Expression =
    Invoke(
      targetObject = path,
      functionName = s"to${CodeGenerator.javaType(elementEncoder.jvmRepr).capitalize}Array",
      dataType = jvmRepr)

  override def toString: String = s"ArrayEncoder($jvmRepr)"
}

case class ObjectArrayEncoder[A]()(implicit
    elementClassTag: ClassTag[A],
    elementEncoder: TypedEncoder[A])
    extends TypedEncoder[Array[A]] {

  override def catalystRepr: DataType =
    ArrayType(elementEncoder.catalystRepr, elementEncoder.nullable)

  override def toCatalyst(path: Expression): Expression = {
    // similar to SeqEncoder
    val mapElement = (path: Expression) =>
      if (elementEncoder.nullable) {
        elementEncoder.toCatalyst(path)
      } else {
        // if element cannot be nullable (not wrapped into option)
        // we want to check it and add nullable=false into catalyst (just like we do in product)
        AssertNotNull(elementEncoder.toCatalyst(path))
      }
    MapObjects(
      function = mapElement,
      inputData = path,
      elementType = elementEncoder.jvmRepr,
      elementNullable = elementEncoder.nullable)
  }

  override def fromCatalyst(path: Expression): Expression = {
    // similar to SeqEncoder but no collection type and unwrap array
    Invoke(
      targetObject = MapObjects(
        function = elementEncoder.fromCatalyst,
        inputData = path,
        elementType = elementEncoder.catalystRepr,
        elementNullable = elementEncoder.nullable),
      functionName = "array",
      dataType = jvmRepr)
  }

  override def toString: String = s"ArrayEncoder($jvmRepr)"
}
