package spark_encoders.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class StaticFieldAccess(
    staticFieldClass: Class[_],
    override val dataType: DataType,
    fieldName: String,
    override val nullable: Boolean = false)
    extends LeafExpression {

  @transient lazy val field =
    try {
      staticFieldClass.getField(fieldName)
    } catch {
      case _: NoSuchFieldException => throw SparkException.internalError(
          s"""A field named "$fieldName" is not declared in $staticFieldClass""")
    }

  override def eval(input: InternalRow): Any =
    field.get(null)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = staticFieldClass.getName
    val resultType = CodeGenerator.javaType(dataType)
    val code = code"""
      final boolean ${ev.isNull} = $nullable;
      final $resultType ${ev.value} = $className.$fieldName;
    """
    ev.copy(code = code)
  }
}
