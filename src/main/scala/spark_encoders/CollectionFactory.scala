package spark_encoders

import scala.collection.{immutable, mutable}
import scala.reflect.ClassTag

// unfortunately Scala Factory is not serializable so we cannot use it
// so there is a list of most commonly used collections
trait CollectionFactory[-Elem, +To] extends Serializable {
  def apply(): mutable.Builder[Elem, To]
}

trait CollectionFactoryPriority2 {

  // immutable seq
  implicit def immutableVectorFactory[A]: CollectionFactory[A, Vector[A]] =
    () => Vector.newBuilder[A]
  implicit def immutableStreamFactory[A]: CollectionFactory[A, Stream[A]] =
    () => Stream.newBuilder[A]
  implicit def immutableQueueFactory[A]: CollectionFactory[A, immutable.Queue[A]] =
    () => immutable.Queue.newBuilder[A]

  // mutable seq
  implicit def mutableListBufferFactory[A]: CollectionFactory[A, mutable.ListBuffer[A]] =
    () => mutable.ListBuffer.newBuilder[A]
  implicit def mutableArraySeqFactory[A: ClassTag]: CollectionFactory[A, mutable.ArraySeq[A]] =
    () => mutable.ArraySeq.newBuilder[A]
  implicit def mutableQueueFactory[A]: CollectionFactory[A, mutable.Queue[A]] =
    () => mutable.Queue.newBuilder[A]
  implicit def mutableArrayStackFactory[A]: CollectionFactory[A, mutable.ArrayStack[A]] =
    () => mutable.ArrayStack.newBuilder[A]

  // immutable set
  implicit def immutableListSetFactory[A]: CollectionFactory[A, immutable.ListSet[A]] =
    () => immutable.ListSet.newBuilder[A]
  implicit def immutableTreeSetFactory[A: Ordering]: CollectionFactory[A, immutable.TreeSet[A]] =
    () => immutable.TreeSet.newBuilder[A]

  // mutable set
  implicit def mutableLinkedHashSetFactory[A]: CollectionFactory[A, mutable.LinkedHashSet[A]] =
    () => mutable.LinkedHashSet.newBuilder[A]
  implicit def mutableTreeSetFactory[A: Ordering]: CollectionFactory[A, mutable.TreeSet[A]] =
    () => mutable.TreeSet.newBuilder[A]

  // immutable map
  implicit def immutableListMapFactory[A, B]: CollectionFactory[(A, B), immutable.ListMap[A, B]] =
    () => immutable.ListMap.newBuilder[A, B]
  implicit def immutableTreeMapFactory[A: Ordering, B]
      : CollectionFactory[(A, B), immutable.TreeMap[A, B]] =
    () => immutable.TreeMap.newBuilder[A, B]

  // mutable map
  implicit def mutableLinkedHashMapFactory[A, B]
      : CollectionFactory[(A, B), mutable.LinkedHashMap[A, B]] =
    () => mutable.LinkedHashMap.newBuilder[A, B]
  implicit def mutableListMapFactory[A, B]: CollectionFactory[(A, B), mutable.ListMap[A, B]] =
    () => mutable.ListMap.newBuilder[A, B]
  implicit def mutableTreeMapFactory[A: Ordering, B]
      : CollectionFactory[(A, B), mutable.TreeMap[A, B]] =
    () => mutable.TreeMap.newBuilder[A, B]
}

trait CollectionFactoryPriority1 extends CollectionFactoryPriority2 {

  // default mutable.Seq impl
  implicit def mutableArrayBufferFactory[A]: CollectionFactory[A, mutable.ArrayBuffer[A]] =
    () => mutable.ArrayBuffer.newBuilder[A]

  // default mutable.Set impl
  implicit def mutableHashSeFactoryt[A]: CollectionFactory[A, mutable.HashSet[A]] =
    () => mutable.HashSet.newBuilder[A]

  // default mutable.Map impl
  implicit def mutableHashMapFactory[A, B]: CollectionFactory[(A, B), mutable.HashMap[A, B]] =
    () => mutable.HashMap.newBuilder[A, B]
}

trait CollectionFactoryPriority0 extends CollectionFactoryPriority1 {

  // default collection.Seq and immutable.Seq impl
  implicit def immutableListFactory[A]: CollectionFactory[A, List[A]] =
    () => List.newBuilder[A]

  // default collection.Set and immutable.Set impl
  implicit def immutableHashSetFactory[A]: CollectionFactory[A, immutable.HashSet[A]] =
    () => immutable.HashSet.newBuilder[A]

  // default collection.Map and immutable.Map impl
  implicit def immutableHashMapFactory[A, B]: CollectionFactory[(A, B), immutable.HashMap[A, B]] =
    () => immutable.HashMap.newBuilder[A, B]
}

object CollectionFactory extends CollectionFactoryPriority0
