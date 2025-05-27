package io.github.pashashiz.spark_encoders

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, CreateNamedStruct, EqualTo, Expression, If, IsNull, Literal, UpCast}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import io.github.pashashiz.spark_encoders.expressions.{AsInstanceOf, ClassSimpleName}

import scala.reflect.ClassTag

class ADTEnumEncoder[A: ClassTag](typeNames: List[String], encoders: => List[TypedEncoder[?]])
    extends TypedEncoder[A] {

  override def catalystRepr: DataType = StringType

  override def toCatalyst(path: Expression): Expression =
    AssertNotNull(ClassSimpleName(path))

  override def fromCatalyst(path: Expression): Expression = {
    val cases = typeNames.zip(encoders)
      .map {
        case (typeName, encoder) =>
          val typeNameLit = Literal(typeName)
          val getInstance = AsInstanceOf(encoder.fromCatalyst(path), jvmRepr)
          (EqualTo(typeNameLit, path), getInstance)
      }
    CaseWhen(cases)
  }
}

class ADTClassEncoder[A: ClassTag](typeNames: List[String], encoders: => List[TypedEncoder[?]])
    extends TypedEncoder[A] {

  private def extractStructFields(encoder: TypedEncoder[?]) =
    encoder.catalystRepr match {
      case StructType(fields) => fields
      case other => throw SparkException.internalError(
          s"ADT case should have StructType schema but was $other")
    }

  override val catalystRepr: DataType = {
    val typeField = StructField("_type", StringType, nullable = false)
    val fields = typeNames.zip(encoders)
      .flatMap {
        case (typeName, encoder) =>
          val fields = extractStructFields(encoder)
          fields.map { field => (typeName, field) }
      }
      .groupBy(_._2.name)
      .toList
      .sortBy(_._1)
      .map {
        case (fieldName, candidates) =>
          val fields = candidates.map(_._2)
          val types = fields.map(_.dataType).distinct
          if (types.size == 1) {
            val required = fields.count(!_.nullable) == typeNames.size
            fields.head.copy(nullable = !required)
          } else {
            throw throw SparkException.internalError(
              s"Standard ADT encoder does not support subtypes that have same field names " +
              s"with different types. Field '$fieldName' has conflicting types: ${types.mkString(", ")}")
          }
      }
    StructType(typeField +: fields)
  }

  override def toCatalyst(path: Expression): Expression = {
    val classSimpleName = AssertNotNull(ClassSimpleName(path))
    val groups = typeNames.zip(encoders)
      .flatMap {
        case (typeName, encoder) =>
          val fields = extractStructFields(encoder)
          val casted = AsInstanceOf(path, encoder.jvmRepr)
          val exprsWithNames = encoder.toCatalyst(casted) match {
            case CreateNamedStruct(children)                   => children
            case If(IsNull(_), _, CreateNamedStruct(children)) => children
            case other => throw throw SparkException.internalError(
                s"ADT case should be encoded as CreateNamedStruct but was $other")
          }
          val exprs = exprsWithNames.zipWithIndex.collect {
            case (expr, index) if index % 2 != 0 => expr
          }
          val typeNameLit = Literal(typeName)
          fields.zip(exprs).map {
            case (field, expr) => (typeNameLit, field, expr)
          }
      }
      .groupBy(_._2.name)
      .toList
      .sortBy(_._1)
    val fields = groups.flatMap {
      case (fieldName, candidates) =>
        val cases = candidates.map {
          case (typeName, _, expr) => (EqualTo(typeName, classSimpleName), expr)
        }
        val caseExpr = CaseWhen(cases)
        val required = candidates.count(!_._3.nullable) == typeNames.size
        val caseExprWithNullability = if (required) AssertNotNull(caseExpr) else caseExpr
        List(Literal(fieldName), caseExprWithNullability)
    }
    val createExpr = CreateNamedStruct(Literal("_type") +: classSimpleName +: fields)
    val nullExpr = Literal.create(null, createExpr.dataType)
    If(IsNull(path), nullExpr, createExpr)
  }

  override def fromCatalyst(path: Expression): Expression = {
    val classSimpleName = UpCast(
      child = UnresolvedExtractValue(child = path, extraction = Literal("_type")),
      target = StringType)
    val cases = typeNames.zip(encoders)
      .map {
        case (typeName, encoder) =>
          val typeNameLit = Literal(typeName)
          val newInstance = AsInstanceOf(encoder.fromCatalyst(path), jvmRepr)
          (EqualTo(typeNameLit, classSimpleName), newInstance)
      }
    CaseWhen(cases)
  }

  override def toString: String = s"ADTClassEncoder($jvmRepr)"
}
