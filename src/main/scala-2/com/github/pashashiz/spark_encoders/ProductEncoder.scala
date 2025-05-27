package com.github.pashashiz.spark_encoders

import com.github.pashashiz.spark_encoders.expressions.ObjectInstance
import magnolia1.CaseClass
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, If, IsNull, KnownNotNull, Literal, UpCast}
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}

import scala.reflect.ClassTag

object ProductEncoder {
  def apply[A: ClassTag](ctx: CaseClass[TypedEncoder, A]): TypedEncoder[A] =
    if (ctx.isObject) new CaseObjectEncoder
    else new CaseClassEncoder(ctx)
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
      val fieldPath = Invoke(
        // set KnownNotNull since there is IsNull check SPARK-26730
        targetObject = KnownNotNull(path),
        functionName = param.label,
        dataType = param.typeclass.jvmRepr,
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
        target = param.typeclass.catalystRepr)
      // we do not accept null values in Product types,
      // nullable fields should use Option instead
      AssertNotNull(param.typeclass.fromCatalyst(paramExpr))
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
