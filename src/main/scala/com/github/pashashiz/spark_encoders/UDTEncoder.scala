package com.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.types.{DataType, ObjectType, UserDefinedType}

import scala.reflect.ClassTag

case class UDTEncoder[A >: Null](udt: UserDefinedType[A])(implicit classTag: ClassTag[A])
    extends TypedEncoder[A] {

  override def catalystRepr: DataType = udt

  private val udtInstance = NewInstance(udt.getClass, Nil, dataType = ObjectType(udt.getClass))

  override def toCatalyst(path: Expression): Expression =
    Invoke(udtInstance, "serialize", udt, Seq(path))

  override def fromCatalyst(path: Expression): Expression =
    Invoke(udtInstance, "deserialize", ObjectType(udt.userClass), Seq(path))
}
