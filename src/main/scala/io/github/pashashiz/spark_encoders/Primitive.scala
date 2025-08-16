package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.types.{ObjectType, _}


object Primitive {

  def isPrimitive(dt: DataType): Boolean =
    CodeGenerator.isPrimitiveType(dt)

  def isUnboxable(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType => true
    case _: DecimalType => false
    case _ => false
  }

  def getClass(dt: DataType): Class[_] =
    CodeGenerator.javaClass(dt)

  def box(expr: Expression, dataType: DataType): Expression = {
    if (isPrimitive(dataType)) {
      StaticInvoke(
        staticObject = getClass(dataType),
        dataType = dataType,
        functionName = "box",
        arguments = expr :: Nil,
        returnNullable = false)
    } else {
      expr
    }
  }

  def unbox(expr: Expression, dataType: DataType): Expression = {
    val method =
      if (isUnboxable(dataType))
        Some(s"${CodeGenerator.javaType(dataType)}Value")
      else
        None
    method.fold(expr)(Invoke(expr, _, dataType))
  }
}
