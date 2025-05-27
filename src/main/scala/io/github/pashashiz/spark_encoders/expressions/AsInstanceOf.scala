package io.github.pashashiz.spark_encoders.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, ObjectType}

case class AsInstanceOf(child: Expression, newDataType: DataType)
    extends UnaryExpression
    with NonSQLExpression {

  val runtimeClass = newDataType match {
    case ObjectType(runtimeClass) => runtimeClass
    case other =>
      throw SparkException.internalError(s"Can only cast object type but received $other")
  }

  override def dataType: DataType = newDataType

  override def eval(input: InternalRow) = {
    val inputObject = child.eval(input)
    if (inputObject == null) {
      null
    } else if (runtimeClass.isAssignableFrom(inputObject.getClass)) {
      inputObject
    } else {
      throw new ClassCastException(
        s"Cannot cast $inputObject of type ${inputObject.getClass} to $runtimeClass")
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputObject = child.genCode(ctx)
    val code = code"""
         final boolean ${ev.isNull} = ${inputObject.isNull};
         ${runtimeClass.getCanonicalName} ${ev.value} = (${runtimeClass.getCanonicalName}) ${inputObject.value};
         """
    ev.copy(code = inputObject.code + code)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def toString: String = s"$child.asInstanceOf[$runtimeClass]"
}
