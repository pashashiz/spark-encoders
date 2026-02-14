package io.github.pashashiz.spark_encoders

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/** Typeclass that witnesses whether a type A is a value class (extends AnyVal). This is resolved at
  * compile time via macros.
  */
trait IsValueClass[A] {
  def isValueClass: Boolean
}

object IsValueClass {
  // Singleton instance - if the implicit resolves, the type is a value class

  def constantFalse[A]: IsValueClass[A] = new IsValueClass[A] { def isValueClass: Boolean = false }

  implicit def materialize[A]: IsValueClass[A] = macro materializeImpl[A]

  def materializeImpl[A: c.WeakTypeTag](c: whitebox.Context): c.Expr[IsValueClass[A]] = {
    import c.universe._

    val tpe = weakTypeOf[A]
    val anyValType = typeOf[AnyVal]

    // Check if the type extends AnyVal (value classes have AnyVal as a base type)
    val isValueClass = tpe <:< anyValType && !(tpe =:= anyValType)

    if (isValueClass) {
      c.Expr[IsValueClass[A]](
        q"new _root_.io.github.pashashiz.spark_encoders.IsValueClass[${weakTypeOf[A]}] { def isValueClass: Boolean = true }")
    } else {
      c.Expr[IsValueClass[A]](
        q"new _root_.io.github.pashashiz.spark_encoders.IsValueClass[${weakTypeOf[A]}] { def isValueClass: Boolean = false }")
    }
  }
}
