package com.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.DataType

import java.io.Serializable
import scala.reflect.ClassTag

trait Invariant[A, B] extends Serializable {
  def map(in: A): B
  def contrMap(out: B): A
}

case class InvariantEncoder[A, B](invariant: Invariant[A, B])(
    implicit
    classTag: ClassTag[A],
    invEncoder: TypedEncoder[B])
    extends TypedEncoder[A] {

  override def nullable = invEncoder.nullable

  override def catalystRepr: DataType = invEncoder.catalystRepr

  override def toCatalyst(path: Expression): Expression = {
    val converted = Invoke(
      targetObject = Literal.fromObject(invariant),
      functionName = "map",
      dataType = invEncoder.jvmRepr,
      arguments = Seq(path),
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

  override def toString: String = s"InvariantEncoder($jvmRepr -> ${invEncoder.jvmRepr})"
}
