package io.github.pashashiz.spark_encoders

import scala.compiletime.*
import scala.quoted.*

/**
 * Typeclass that witnesses whether a type A is a value class (extends AnyVal).
 * This is resolved at compile time.
 */
sealed trait IsValueClass[A]

object IsValueClass:
  private[spark_encoders] object Instance extends IsValueClass[Any]

  /**
   * Compile-time derivation for value classes.
   * Only succeeds if A extends AnyVal.
   */
  inline given derived[A <: AnyVal]: IsValueClass[A] = Instance.asInstanceOf[IsValueClass[A]]
