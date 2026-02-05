package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.expressions.{AsInstanceOf, ObjectInstance}
import magnolia1.CaseClass
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, If, IsNull, KnownNotNull, Literal, UpCast}
import org.apache.spark.sql.types.{DataType, Metadata, ObjectType, StructField, StructType}

import scala.reflect.ClassTag

object ProductEncoder {

  /** Recursively make all nested struct fields nullable for UpCast compatibility.
    * Spark stores nested structs as nullable by default, so when reading back,
    * we need the target schema to accept nullable struct fields. */
  def makeNullable(dt: DataType): DataType = dt match {
    case st: StructType =>
      StructType(st.fields.map { f =>
        f.copy(dataType = makeNullable(f.dataType), nullable = true)
      })
    case other => other
  }

  def apply[A: ClassTag](ctx: CaseClass[TypedEncoder, A]): TypedEncoder[A] =
    if (ctx.isValueClass) new ValueClassEncoder(ctx)
    else if (ctx.isObject) new CaseObjectEncoder
    else new CaseClassEncoder(ctx)
}

class ValueClassEncoder[A](ctx: CaseClass[TypedEncoder, A])(implicit val A: ClassTag[A])
    extends TypedEncoder[A] {

  private val param = ctx.parameters.head
  private val underlyingJvmRepr = param.typeclass.jvmRepr

  override def catalystRepr: DataType = param.typeclass.catalystRepr

  // For top-level use (e.g., Dataset[Foo]), report the boxed type
  // jvmRepr defaults to ObjectType(A.runtimeClass), which is correct

  // For field access (e.g., baz.foo), value classes are erased to the underlying type
  override def fieldAccessJvmRepr: DataType = underlyingJvmRepr

  override def toCatalyst(path: Expression): Expression = {
    // Handle multiple contexts:
    // - Top-level (boxed): path is Foo, need to extract via .value()
    // - Field access (erased): path is already String
    // We use the path's dataType to detect which case we're in
    val underlyingPath = path.dataType match {
      case ObjectType(cls) if cls == A.runtimeClass =>
        // Boxed value class - extract the underlying value
        Invoke(path, param.label, underlyingJvmRepr)
      case _ =>
        // Already the underlying type (erased context)
        path
    }
    param.typeclass.toCatalyst(underlyingPath)
  }

  override def fromCatalyst(path: Expression): Expression = {
    // For top-level use, box the underlying value into the value class
    NewInstance(
      A.runtimeClass,
      Seq(param.typeclass.fromCatalyst(path)),
      jvmRepr)
  }

  override def fromCatalystForField(path: Expression): Expression = {
    // For field use in case class constructors, return the underlying value
    // (the parent constructor takes the erased type)
    param.typeclass.fromCatalyst(path)
  }

  override def toString: String = s"ValueClassEncoder($jvmRepr)"

}

class CaseObjectEncoder[A: ClassTag] extends TypedEncoder[A] {

  override def catalystRepr: DataType = StructType(Seq.empty)

  override def toCatalyst(path: Expression): Expression =
    CreateNamedStruct(Seq.empty)

  override def fromCatalyst(path: Expression): Expression =
    ObjectInstance(runtimeClass)

  override def toString: String = s"CaseObjectEncoder($jvmRepr)"
}

class CaseClassEncoder[A: ClassTag](ctx: CaseClass[TypedEncoder, A]) extends TypedEncoder[A] {

  private def isValueClassEncoder(encoder: TypedEncoder[_]): Boolean = encoder match {
    case _: ValueClassEncoder[_]      => true
    case inv: InvariantEncoder[_, _]  => inv.isValueClass
    case _                            => false
  }

  private def fieldReturnType(label: String): Class[_] =
    runtimeClass.getMethod(label).getReturnType

  override def catalystRepr: DataType = {
    val fields = ctx.parameters.map { field =>
      StructField(
        name = field.label,
        dataType = field.typeclass.catalystRepr,
        nullable = field.typeclass.nullable,
        metadata = Metadata.empty)
    }
    StructType(fields)
  }

  override def toCatalyst(path: Expression): Expression = {
    val nameExprs = ctx.parameters.map(param => Literal(param.label))
    val valueExprs = ctx.parameters.map { param =>
      val returnType = fieldReturnType(param.label)
      val fieldAccessRepr =
        if (isValueClassEncoder(param.typeclass) && returnType == classOf[Object]) {
          // Generic value class fields are boxed; use jvmRepr so ValueClassEncoder can unbox.
          param.typeclass.jvmRepr
        } else {
          param.typeclass.fieldAccessJvmRepr
        }
      val fieldPath = Invoke(
        // set KnownNotNull since there is IsNull check SPARK-26730
        targetObject = KnownNotNull(path),
        functionName = param.label,
        // Use fieldAccessJvmRepr to handle value class erasure
        dataType = fieldAccessRepr,
        arguments = Nil,
        // this is required to property generate NPE if result is null
        returnNullable = true)
      // we do not accept null values in Product types,
      // nullable fields should use Option instead
      param.typeclass.toCatalyst(AssertNotNull(fieldPath))
    }
    val exprs = nameExprs.zip(valueExprs).flatMap {
      case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
    }
    val createExpr = CreateNamedStruct(exprs)
    val nullExpr = Literal.create(null, createExpr.dataType)
    If(IsNull(path), nullExpr, createExpr)
  }

  override def fromCatalyst(path: Expression): Expression = {
    val exprs = ctx.parameters.map { param =>
      val paramExpr = UpCast(
        child = UnresolvedExtractValue(child = path, extraction = Literal(param.label)),
        // Use makeNullable for nested structs since Spark stores them as nullable by default
        target = ProductEncoder.makeNullable(param.typeclass.catalystRepr))
      // we do not accept null values in Product types,
      // nullable fields should use Option instead
      // Use fromCatalystForField to handle value class erasure in constructor args
      val returnType = fieldReturnType(param.label)
      val decoded =
        if (isValueClassEncoder(param.typeclass) && returnType == classOf[Object]) {
          // Generic value class fields expect boxed values.
          param.typeclass.fromCatalyst(paramExpr)
        } else {
          param.typeclass.fromCatalystForField(paramExpr)
        }
      AssertNotNull(decoded)
    }
    val newExpr = NewInstance(
      cls = runtimeClass,
      arguments = exprs,
      dataType = jvmRepr,
      propagateNull = true)
    val nullExpr = Literal.create(null, jvmRepr)
    If(IsNull(path), nullExpr, newExpr)
  }

  override def toString: String = s"CaseClassEncoder($jvmRepr)"
}
