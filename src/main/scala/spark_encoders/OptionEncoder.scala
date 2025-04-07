package spark_encoders

import org.apache.spark.sql.catalyst.encoders.EncoderUtils
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{UnwrapOption, WrapOption}
import org.apache.spark.sql.types.{DataType, ObjectType}

import scala.reflect.ClassTag

case class OptionEncoder[A]()(implicit inner: TypedEncoder[A]) extends TypedEncoder[Option[A]] {

  override def nullable = true

  override def catalystRepr: DataType = inner.catalystRepr

  override def toCatalyst(path: Expression): Expression = {
    // note: we want to make sure we always get Object type here,
    // for example, when we have primitive type such as IntegerType,
    // we want to end up having ObjectType(java.lang.Integer) which is translated to java.lang.Integer
    // instead of IntegerType which is translated to int
    val optionType = ObjectType(EncoderUtils.javaBoxedType(inner.jvmRepr))
    val unwrapped = UnwrapOption(optionType, path)
    // note: unboxed is noop for objects
    val unboxed = Primitive.unbox(unwrapped, catalystRepr)
    // note: we do not add IfNull expression cause usually inner Expression
    // should tolerate null input argument and should short circuit and return null already (such as Invoke)
    // yet if we hit the case when that is not true, we might add it here
    inner.toCatalyst(unboxed)
  }

  override def fromCatalyst(path: Expression): Expression = {
    WrapOption(inner.fromCatalyst(path), inner.jvmRepr)
  }

  override def toString: String = s"OptionEncoder(${inner.jvmRepr})"
}
