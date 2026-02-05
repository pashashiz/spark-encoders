package io.github.pashashiz.spark_encoders

import scala.compiletime.*
import scala.quoted.*

/**
 * Typeclass that witnesses whether a type A is a value class (extends AnyVal).
 * This is resolved at compile time.
 */
trait IsValueClass[A]:
  def isValueClass: Boolean

object IsValueClass extends IsValueClassLowPriority:

  def constantFalse[A]: IsValueClass[A] = new IsValueClass[A] { def isValueClass: Boolean = false }

  /**
   * Compile-time derivation for value classes.
   * Only succeeds if A extends AnyVal.
   */
  given derivedValueClass[A <: AnyVal]: IsValueClass[A] = new IsValueClass[A] { def isValueClass: Boolean = true }


trait IsValueClassLowPriority:

  given derivedAny[A]: IsValueClass[A] = new IsValueClass[A] { def isValueClass: Boolean = false }
