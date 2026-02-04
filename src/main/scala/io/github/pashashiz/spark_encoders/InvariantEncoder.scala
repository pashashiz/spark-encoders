package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{DataType, ObjectType}

import java.io.Serializable
import scala.reflect.ClassTag

trait Invariant[A, B] extends Serializable {
  def map(in: A): B
  def contrMap(out: B): A
}

class InvariantEncoder[A, B](
    val invariant: Invariant[A, B],
    val isValueClass: Boolean
)(implicit
    classTag: ClassTag[A],
    invEncoder: TypedEncoder[B])
    extends TypedEncoder[A] {

  override def nullable = invEncoder.nullable

  override def catalystRepr: DataType = invEncoder.catalystRepr

  // For value classes, field access returns the underlying type due to erasure
  override def fieldAccessJvmRepr: DataType =
    if (isValueClass) invEncoder.jvmRepr else jvmRepr

  override def toCatalyst(path: Expression): Expression = {
    // For value classes, handle both boxed and erased contexts
    val inputPath = if (isValueClass) {
      path.dataType match {
        case ObjectType(cls) if cls == classTag.runtimeClass =>
          // Boxed value class - pass as-is for the invariant.map call
          path
        case _ =>
          // Erased context - need to box it first for the invariant.map call
          NewInstance(classTag.runtimeClass, Seq(path), jvmRepr)
      }
    } else {
      path
    }

    val converted = Invoke(
      targetObject = Literal.fromObject(invariant),
      functionName = "map",
      dataType = invEncoder.jvmRepr,
      arguments = Seq(inputPath),
      returnNullable = false)
    invEncoder.toCatalyst(converted)
  }

  override def fromCatalyst(path: Expression): Expression = {
    val decoded = invEncoder.fromCatalyst(path)
    Invoke(
      targetObject = Literal.fromObject(invariant),
      functionName = "contrMap",
      dataType = jvmRepr,
      arguments = Seq(decoded),
      returnNullable = false)
  }

  override def fromCatalystForField(path: Expression): Expression = {
    // For value class fields in case classes, return the underlying type
    // (the parent constructor takes the erased type)
    if (isValueClass) {
      invEncoder.fromCatalyst(path)
    } else {
      fromCatalyst(path)
    }
  }

  override def toString: String = s"InvariantEncoder($jvmRepr -> ${invEncoder.jvmRepr})"
}

object InvariantEncoder {
  /** Create an InvariantEncoder, auto-detecting if A is a value class via runtime reflection */
  def apply[A, B](invariant: Invariant[A, B])(implicit
      classTag: ClassTag[A],
      invEncoder: TypedEncoder[B]
  ): InvariantEncoder[A, B] = {
    // Runtime detection: value classes have a single-parameter constructor matching the target type
    val isValueClass = {
      val constructors = classTag.runtimeClass.getConstructors
      constructors.length == 1 &&
        constructors.head.getParameterCount == 1 &&
        constructors.head.getParameterTypes.head == invEncoder.classTag.runtimeClass
    }
    new InvariantEncoder(invariant, isValueClass)
  }

  /** Create an InvariantEncoder for a value class (compile-time verified) */
  def forValueClass[A, B](invariant: Invariant[A, B])(implicit
      classTag: ClassTag[A],
      invEncoder: TypedEncoder[B],
      isVC: IsValueClass[A]
  ): InvariantEncoder[A, B] = {
    new InvariantEncoder(invariant, isValueClass = true)
  }
}
