package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal

import scala.annotation.nowarn
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticExpressionPathEncoder, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.reflect.ClassTag

object Shim {

  def staticInvoke(
      staticObject: Class[_],
      dataType: DataType,
      functionName: String,
      arguments: Seq[Expression],
      propagateNull: Boolean = true,
      returnNullable: Boolean = true,
      isDeterministic: Boolean = true): Expression =
    StaticInvoke(
      staticObject = staticObject,
      dataType = dataType,
      functionName = functionName,
      arguments = arguments,
      inputTypes = Nil,
      propagateNull = Boolean.box(propagateNull),
      returnNullable = Boolean.box(returnNullable),
      isDeterministic = Boolean.box(isDeterministic),
      scalarFunction = Option.empty)

  /** SparkClosureCleaner has private access so use reflection */
  def cleanClosure(
      closure: AnyRef,
      checkSerializable: Boolean = true,
      cleanTransitively: Boolean = true): Unit = {
    val cleanerClass = Class.forName("org.apache.spark.util.SparkClosureCleaner$")
    val companionObject = cleanerClass.getField("MODULE$").get(null)
    val cleanMethod = cleanerClass.getDeclaredMethod(
      "clean",
      classOf[AnyRef],
      classOf[Boolean],
      classOf[Boolean])
    cleanMethod.invoke(
      companionObject,
      closure,
      Boolean.box(checkSerializable),
      Boolean.box(cleanTransitively))
  }

  def expressionEncoder[A](encoder: Encoder[A]): ExpressionEncoder[A] = {
    encoder match {
      case ee: ExpressionEncoder[A] => ee
      case ag: AgnosticEncoder[A] =>
        ExpressionEncoder[A](ag)
      case other =>
        throw new RuntimeException(s"Unsupported encoder type: ${other.getClass.getName} ($other)")
    }
  }

  def expressionEncoder[A](encoder: TypedEncoder[A]): ExpressionEncoder[A] = {
    // input is always BoundReference with single element
    val in = BoundReference(0, encoder.jvmRepr, encoder.nullable)
    // output is always GetColumnByOrdinal with single element
    val out = GetColumnByOrdinal(0, encoder.catalystRepr)
    @nowarn("cat=deprecation")
    val agnosticEncoder = new AgnosticExpressionPathEncoder[A] {
      override def isPrimitive: Boolean = Types.isPrimitive(dataType)
      override def nullable: Boolean = encoder.nullable
      override def dataType: DataType = encoder.catalystRepr
      override def clsTag: ClassTag[A] = encoder.classTag
      override def isStruct: Boolean =
        dataType match {
          case _: StructType => true
          case _             => false
        }
      override def toCatalyst(input: Expression): Expression =
        encoder.toCatalyst(input)
      override def fromCatalyst(inputPath: Expression): Expression =
        encoder.fromCatalyst(inputPath)
    }
    new ExpressionEncoder[A](
      encoder = agnosticEncoder,
      objSerializer = encoder.toCatalyst(in),
      objDeserializer = encoder.fromCatalyst(out))
  }
}
