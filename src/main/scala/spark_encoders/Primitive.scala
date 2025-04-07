package spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.javaType
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.types.{ObjectType, _}
import org.apache.spark.unsafe.types.UTF8String

object Primitive {

  def isPrimitive(dt: DataType): Boolean =
    CodeGenerator.isPrimitiveType(dt)

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
      if (isPrimitive(dataType))
        Some(s"${CodeGenerator.javaType(dataType)}Value")
      else
        None
    method.fold(expr)(Invoke(expr, _, dataType))
  }
}
