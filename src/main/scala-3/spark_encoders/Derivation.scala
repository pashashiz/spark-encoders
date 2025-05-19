package spark_encoders

import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.compiletime._
import scala.deriving._

trait Derivation {

  inline given derived[A](using m: Mirror.Of[A], classTag: ClassTag[A]): TypedEncoder[A] =
    derive

  inline def derive[T](using m: Mirror.Of[T], classTag: ClassTag[T]): TypedEncoder[T] = {
    lazy val elemInstances = summonInstances[T, m.MirroredElemTypes]
    val labels = constValueList[m.MirroredElemLabels]
    inline m match {
      case s: Mirror.SumOf[T]     => typeEncoderSum[T](s, labels, elemInstances)
      case p: Mirror.ProductOf[T] => typeEncoderProduct[T](p, labels, elemInstances)
    }
  }

  private inline def constValueList[T <: Tuple]: List[String] =
    inline erasedValue[T] match {
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => constValue[head].toString :: constValueList[tail]
    }

  private inline def summonInstances[T, Elems <: Tuple]: List[TypedEncoder[?]] =
    inline erasedValue[Elems] match
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => deriveOrSummon[T, head] :: summonInstances[T, tail]

  private inline def deriveOrSummon[T, Elem]: TypedEncoder[Elem] =
    inline erasedValue[Elem] match
      case _: T => deriveRec[T, Elem]
      case _    => summonInline[TypedEncoder[Elem]]

  private inline def deriveRec[T, Elem]: TypedEncoder[Elem] =
    inline erasedValue[T] match
      case _: Elem => error("infinite recursive derivation")
      case _ => Derivation.derive[Elem](using
          summonInline[Mirror.Of[Elem]],
          summonInline[ClassTag[Elem]])

  private inline def typeEncoderProduct[T](
      p: Mirror.ProductOf[T],
      labels: List[String],
      instances: => List[TypedEncoder[?]])(implicit classTag: ClassTag[T]): TypedEncoder[T] =
    if (isSingleton[T]) new CaseObjectEncoder[T]
    else new CaseClassEncoder(labels, instances)

  private inline def typeEncoderSum[T](
      s: Mirror.SumOf[T],
      labels: List[String],
      elems: => List[TypedEncoder[?]])(implicit classTag: ClassTag[T]): TypedEncoder[T] =
    if (isEnum[s.MirroredElemTypes]) new ADTEnumEncoder(labels, elems)
    else new ADTClassEncoder(labels, elems)

  private inline def isEnum[Elems <: Tuple]: Boolean =
    inline erasedValue[Elems] match
      case _: EmptyTuple     => true
      case _: (head *: tail) => isSingleton[head] && isEnum[tail]

  private inline def isSingleton[Elem]: Boolean =
    inline summonInline[Mirror.Of[Elem]] match {
      case m: Mirror.Singleton      => true
      case m: Mirror.SingletonProxy => true
      case _                        => false
    }
}

object Derivation extends Derivation
