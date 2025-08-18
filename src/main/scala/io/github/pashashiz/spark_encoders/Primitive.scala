package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.compatibility.staticInvoke
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types._


object Primitive {

  def isPrimitive(dt: DataType): Boolean =
    CodeGenerator.isPrimitiveType(dt)

  def isUnboxable(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType => true
    case _ => false
  }

  def getClass(dt: DataType): Class[_] =
    CodeGenerator.javaClass(dt)

  def box(expr: Expression, dataType: DataType): Expression = {
    if (isPrimitive(dataType)) {
      staticInvoke(
        staticObject = getClass(dataType),
        dataType = dataType,
        functionName = "box",
        arguments = expr :: Nil,
        returnNullable = false)
    } else {
      expr
    }
  }

  /** For boxed Java primitive types, we can safely take the boxed value.
   *  However, we need to take special care with Spark data types that map
   *  to Java primitives. For example, [[java.time.Instant]] maps to [[org.apache.spark.sql.types.TimestampType]]
   *  which maps to Java primitive long. We can't unbox these, but Spark's
   *  [[org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.isPrimitiveType]] would return true.
   */
  def unbox(expr: Expression, dataType: DataType): Expression = {
    val method =
      if (isUnboxable(dataType))
        Some(s"${CodeGenerator.javaType(dataType)}Value")
      else
        None
    method.fold(expr)(Invoke(expr, _, dataType))
  }
}
