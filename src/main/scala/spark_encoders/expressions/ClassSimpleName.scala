package spark_encoders.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class ClassSimpleName(child: Expression, stripDollar: Boolean = true)
    extends UnaryExpression
    with NonSQLExpression {

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    val inputObject = child.eval(input)
    if (inputObject == null) {
      null
    } else {
      val name = inputObject.getClass.getSimpleName
      val nameNoDollar =
        if (stripDollar && name.charAt(name.length - 1) == '$')
          name.substring(0, name.length - 1)
        else
          name
      UTF8String.fromString(nameNoDollar)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputObject = child.genCode(ctx)
    val className = ctx.freshName("className")
    val classNameCode =
      if (stripDollar) {
        code"""
           String $className = ${inputObject.value}.getClass().getSimpleName();
           if ($className.charAt($className.length() - 1) == '$$') {
             $className = $className.substring(0, $className.length() - 1);
           }
        """
      } else {
        code"""
           String $className = ${inputObject.value}.getClass().getSimpleName();
        """
      }
    val code =
      code"""
         final boolean ${ev.isNull} = ${inputObject.isNull};
         org.apache.spark.unsafe.types.UTF8String ${ev.value} = null;
         if (!${ev.isNull}) {
           $classNameCode
           ${ev.value} = org.apache.spark.unsafe.types.UTF8String.fromString($className);
         }
         """
    ev.copy(code = inputObject.code + code)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def toString: String = s"$child.classSimpleName"
}
