package io.github.pashashiz.spark_encoders

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/**
 * Typeclass that witnesses whether a type A is a value class (extends AnyVal).
 * This is resolved at compile time via macros.
 */
sealed trait IsValueClass[A]

object IsValueClass {
  // Singleton instance - if the implicit resolves, the type is a value class
  private[spark_encoders] object Instance extends IsValueClass[Any]

  implicit def materialize[A]: IsValueClass[A] = macro materializeImpl[A]

  def materializeImpl[A: c.WeakTypeTag](c: whitebox.Context): c.Expr[IsValueClass[A]] = {
    import c.universe._

    val tpe = weakTypeOf[A]
    val anyValType = typeOf[AnyVal]

    // Check if the type extends AnyVal (value classes have AnyVal as a base type)
    val isValueClass = tpe <:< anyValType && !(tpe =:= anyValType)

    if (isValueClass) {
      c.Expr[IsValueClass[A]](
        q"_root_.io.github.pashashiz.spark_encoders.IsValueClass.Instance.asInstanceOf[_root_.io.github.pashashiz.spark_encoders.IsValueClass[$tpe]]"
      )
    } else {
      c.abort(c.enclosingPosition, s"${tpe.typeSymbol.name} is not a value class")
    }
  }
}
