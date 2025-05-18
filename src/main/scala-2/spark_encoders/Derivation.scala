package spark_encoders

import scala.language.experimental.macros
import scala.reflect.ClassTag

import magnolia1.CaseClass
import magnolia1.SealedTrait
import magnolia1.Magnolia

trait Derivation {
  type Typeclass[T] = TypedEncoder[T]
  def join[A: ClassTag](ctx: CaseClass[TypedEncoder, A]): TypedEncoder[A] = ProductEncoder[A](ctx)
  def split[A: ClassTag](ctx: SealedTrait[TypedEncoder, A]): TypedEncoder[A] = ADTEncoder[A](ctx)
  implicit def genTypedEncoder[T]: TypedEncoder[T] = macro Magnolia.gen[T]
}
