package com.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, MapObjects, NewInstance}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.TraversableOnce
import scala.language.higherKinds
import scala.reflect.ClassTag

// note: converting to targetCollection is actually the best effort
// that is why this encoder returns collection.Seq:
// - in interpreted mode collection.Seq is always returned
// - in codegen targetCollection.newBuilder is generated
case class SeqEncoder[C[_] <: collection.Seq[_], A]()(implicit
    targetCollection: ClassTag[C[A]],
    elementEncoder: TypedEncoder[A])
    extends TypedEncoder[collection.Seq[A]] {

  override def catalystRepr: DataType =
    ArrayType(elementEncoder.catalystRepr, elementEncoder.nullable)

  // catalyst represents array as ArrayData
  override def toCatalyst(path: Expression): Expression =
    if (Primitive.isPrimitive(elementEncoder.jvmRepr)) {
      // skip map phase
      // note: check to see if we can store it as unsafe array data
      NewInstance(classOf[GenericArrayData], path :: Nil, catalystRepr)
    } else {
      val mapElement = (path: Expression) =>
        if (elementEncoder.nullable) {
          elementEncoder.toCatalyst(path)
        } else {
          // if element cannot be nullable (not wrapped into option)
          // we want to check it and add nullable=false into catalyst (just like we do in product)
          AssertNotNull(elementEncoder.toCatalyst(path))
        }
      // MapObjects produces ArrayData, for non-primitive that is GenericArrayData
      // UnsafeArrayData is used for primitive arrays, check to see if we can use that for Seq too
      MapObjects(
        function = mapElement,
        inputData = path,
        elementType = elementEncoder.jvmRepr,
        elementNullable = elementEncoder.nullable)
    }

  override def fromCatalyst(path: Expression): Expression =
    MapObjects(
      function = elementEncoder.fromCatalyst,
      inputData = path,
      elementType = elementEncoder.catalystRepr,
      elementNullable = elementEncoder.nullable,
      customCollectionCls = Some(targetCollection.runtimeClass))

  override def toString: String = s"SeqEncoder($jvmRepr)"
}

// in case produced Seq is not a subtype of expected, upcast using builder
class SeqInvariant[C[_] <: collection.Seq[_], A](factory: CollectionFactory[A, C[A]])(
    implicit collectionClass: ClassTag[C[A]])
    extends Invariant[C[A], collection.Seq[A]] {
  override def map(in: C[A]): collection.Seq[A] =
    in.asInstanceOf[collection.Seq[A]]
  override def contrMap(out: collection.Seq[A]): C[A] = {
    if (collectionClass.runtimeClass.isAssignableFrom(out.getClass)) {
      out.asInstanceOf[C[A]]
    } else {
      val builder = factory()
      builder.sizeHint(out)
      builder ++= out
      builder.result
    }
  }
  override def toString = s"SeqInvariant(${collectionClass.runtimeClass.getCanonicalName})"
}

class SetInvariant[C[_] <: collection.Set[_], A](factory: CollectionFactory[A, C[A]])(
    implicit collectionClass: ClassTag[C[A]])
    extends Invariant[C[A], collection.Seq[A]] {
  override def map(in: C[A]): collection.Seq[A] =
    in.toSeq.asInstanceOf[Seq[A]]
  override def contrMap(out: collection.Seq[A]): C[A] = {
    val builder = factory()
    builder.sizeHint(out)
    builder ++= out
    builder.result
  }
  override def toString = s"SetInvariant(${collectionClass.runtimeClass.getCanonicalName})"
}
