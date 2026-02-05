package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, If, IsNull, KnownNotNull, Literal, UpCast}
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import io.github.pashashiz.spark_encoders.expressions.ObjectInstance

import scala.reflect.ClassTag

object ProductEncoder:
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


class CaseObjectEncoder[A: ClassTag] extends TypedEncoder[A] {

  override def catalystRepr: DataType = StructType(Seq.empty)

  override def toCatalyst(path: Expression): Expression =
    CreateNamedStruct(Seq.empty)

  override def fromCatalyst(path: Expression): Expression =
    ObjectInstance(runtimeClass)

  override def toString: String = s"CaseObjectEncoder($jvmRepr)"
}

class CaseClassEncoder[A: ClassTag](
    labels: List[String],
    encoders: => List[TypedEncoder[?]]) extends TypedEncoder[A] {

  override def catalystRepr: DataType = {
    val fields = labels.zip(encoders).map {
      case (label, encoder) =>
        StructField(
          name = label,
          dataType = encoder.catalystRepr,
          nullable = encoder.nullable,
          metadata = Metadata.empty)
    }
    StructType(fields)
  }

  override def toCatalyst(path: Expression): Expression = {
    val nameExprs = labels.map(label => Literal(label))
    val valueExprs = labels.zip(encoders).map {
      case (label, encoder) =>
        val fieldPath = Invoke(
          // set KnownNotNull since there is IsNull check SPARK-26730
          targetObject = KnownNotNull(path),
          functionName = label,
          dataType = encoder.jvmRepr,
          arguments = Nil,
          // this is required to property generate NPE if result is null
          returnNullable = true)
        // we do not accept null values in Product types,
        // nullable fields should use Option instead
        encoder.toCatalyst(AssertNotNull(fieldPath))
    }
    val exprs = nameExprs.zip(valueExprs).flatMap {
      case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
    }
    val createExpr = CreateNamedStruct(exprs)
    val nullExpr = Literal.create(null, createExpr.dataType)
    If(IsNull(path), nullExpr, createExpr)
  }

  override def fromCatalyst(path: Expression): Expression = {
    val exprs = labels.zip(encoders).map {
      case (label, encoder) =>
        val paramExpr = UpCast(
          child = UnresolvedExtractValue(child = path, extraction = Literal(label)),
          // Use makeNullable for nested structs since Spark stores them as nullable by default
          target = ProductEncoder.makeNullable(encoder.catalystRepr))
        // we do not accept null values in Product types,
        // nullable fields should use Option instead
        AssertNotNull(encoder.fromCatalyst(paramExpr))
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
