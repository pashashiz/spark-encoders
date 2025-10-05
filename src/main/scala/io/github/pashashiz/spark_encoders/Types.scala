package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.Shim.staticInvoke
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object Types {

  def isPrimitive(dt: DataType): Boolean =
    CodeGenerator.isPrimitiveType(dt)

  def isUnboxable(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
      true
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

  /** For boxed Java primitive types, we can safely take the boxed value. However, we need to take
    * special care with Spark data types that map to Java primitives. For example,
    * [[java.time.Instant]] maps to [[org.apache.spark.sql.types.TimestampType]] which maps to Java
    * primitive long. We can't unbox these, but Spark's
    * [[org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.isPrimitiveType]] would
    * return true.
    */
  def unbox(expr: Expression, dataType: DataType): Expression = {
    val method =
      if (isUnboxable(dataType))
        Some(s"${CodeGenerator.javaType(dataType)}Value")
      else
        None
    method.fold(expr)(Invoke(expr, _, dataType))
  }

  // Methods below are not available in Spark 3.3
  // once we drop it we can replace with existing spark implementation

  // Exists in EncoderUtils.typeBoxedJavaMapping
  private val typeBoxedJavaMapping: Map[DataType, Class[_]] = Map[DataType, Class[_]](
    BooleanType -> classOf[java.lang.Boolean],
    ByteType -> classOf[java.lang.Byte],
    ShortType -> classOf[java.lang.Short],
    IntegerType -> classOf[java.lang.Integer],
    LongType -> classOf[java.lang.Long],
    FloatType -> classOf[java.lang.Float],
    DoubleType -> classOf[java.lang.Double],
    DateType -> classOf[java.lang.Integer],
    TimestampType -> classOf[java.lang.Long])

  // Exists in EncoderUtils.javaBoxedType
  @scala.annotation.tailrec
  def javaBoxedType(dt: DataType): Class[_] = dt match {
    case _: DecimalType           => classOf[Decimal]
    case _: DayTimeIntervalType   => classOf[java.lang.Long]
    case _: YearMonthIntervalType => classOf[java.lang.Integer]
    case BinaryType               => classOf[Array[Byte]]
    case _: StringType            => classOf[UTF8String]
    case CalendarIntervalType     => classOf[CalendarInterval]
    case _: StructType            => classOf[InternalRow]
    case _: ArrayType             => classOf[ArrayData]
    case _: MapType               => classOf[MapData]
    case udt: UserDefinedType[_]  => javaBoxedType(udt.sqlType)
    case ObjectType(cls)          => cls
    case _                        => typeBoxedJavaMapping.getOrElse(dt, classOf[java.lang.Object])
  }
}
