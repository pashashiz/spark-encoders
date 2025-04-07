package spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, CatalystToExternalMap, ExternalMapToCatalyst, UnresolvedCatalystToExternalMap}
import org.apache.spark.sql.types.{DataType, MapType}

import scala.collection.{immutable, mutable}
import scala.language.higherKinds
import scala.reflect.ClassTag

case class MapEncoder[C[_, _] <: collection.Map[_, _], A, B]()(implicit
    targetCollection: ClassTag[C[A, B]],
    keyEncoder: TypedEncoder[A],
    valueEncoder: TypedEncoder[B])
    extends TypedEncoder[collection.Map[A, B]] {

  override def catalystRepr: DataType =
    MapType(keyEncoder.catalystRepr, valueEncoder.catalystRepr, valueEncoder.nullable)

  // catalyst represents it as new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
  override def toCatalyst(path: Expression): Expression = {
    val mapValue = (path: Expression) =>
      if (valueEncoder.nullable) {
        valueEncoder.toCatalyst(path)
      } else {
        // if element cannot be nullable (not wrapped into option)
        // we want to check it and add nullable=false into catalyst (just like we do in product)
        AssertNotNull(valueEncoder.toCatalyst(path))
      }
    ExternalMapToCatalyst(
      inputMap = path,
      keyType = keyEncoder.jvmRepr,
      keyConverter = keyEncoder.toCatalyst,
      keyNullable = false,
      valueType = valueEncoder.jvmRepr,
      valueConverter = mapValue,
      valueNullable = valueEncoder.nullable)
  }

  override def fromCatalyst(path: Expression): Expression = {
    // spark relies on Map.newBuilder() without Ordering param
    // so we cannot create TreeMap from Spark and need to copy into it after
    val collClass = targetCollection.runtimeClass.getSimpleName match {
      case "TreeMap" => classOf[mutable.HashMap[_, _]]
      case _         => targetCollection.runtimeClass
    }
    CatalystToExternalMap(
      UnresolvedCatalystToExternalMap(
        child = path,
        keyFunction = keyEncoder.fromCatalyst,
        valueFunction = valueEncoder.fromCatalyst,
        collClass = collClass))
  }
}

// in case produced Map is not a subtype of expected, upcast using builder (i.e. TreeMap)
class MapInvariant[C[_, _] <: collection.Map[_, _], A, B](
    factory: CollectionFactory[(A, B), C[A, B]])(
    implicit collectionClass: ClassTag[C[A, B]])
    extends Invariant[C[A, B], collection.Map[A, B]] {
  override def map(in: C[A, B]): collection.Map[A, B] =
    in.asInstanceOf[collection.Map[A, B]]
  override def contrMap(out: collection.Map[A, B]): C[A, B] = {
    if (collectionClass.runtimeClass.isAssignableFrom(out.getClass)) {
      out.asInstanceOf[C[A, B]]
    } else {
      val builder = factory()
      builder.sizeHint(out)
      builder ++= out
      builder.result
    }
  }
  override def toString = s"MapInvariant(${collectionClass.runtimeClass.getCanonicalName})"
}
