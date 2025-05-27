package com.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

case class ExternalEncoder[T: ClassTag](external: ExpressionEncoder[T]) extends TypedEncoder[T] {

  override def catalystRepr: DataType = {
    external.objSerializer.dataType
  }

  override def toCatalyst(path: Expression): Expression = {
    external.objSerializer.transformUp {
      case BoundReference(0, _, _) => path
    }
  }

  override def fromCatalyst(path: Expression): Expression = {
    external.objDeserializer.transformUp {
      case GetColumnByOrdinal(0, _) => path
    }
  }
}
