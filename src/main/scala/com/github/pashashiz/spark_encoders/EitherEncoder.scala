package com.github.pashashiz.spark_encoders

import java.io.Serializable

sealed trait EitherStruct[+A, +B] extends Product with Serializable {
  def toEither: Either[A, B]
}

object EitherStruct {
  case class Left[+A, +B](left: A) extends EitherStruct[A, B] {
    override def toEither: Either[A, B] = scala.util.Left(left)
  }
  case class Right[+A, +B](right: B) extends EitherStruct[A, B] {
    override def toEither: Either[A, B] = scala.util.Right(right)
  }
  def fromEither[A, B](either: Either[A, B]): EitherStruct[A, B] =
    either match {
      case scala.util.Left(value)  => Left(value)
      case scala.util.Right(value) => Right(value)
    }
}

class EitherInvariant[A, B] extends Invariant[Either[A, B], EitherStruct[A, B]] {
  override def map(in: Either[A, B]): EitherStruct[A, B] = EitherStruct.fromEither(in)
  override def contrMap(out: EitherStruct[A, B]): Either[A, B] = out.toEither
  override def toString = s"EitherInvariant"
}
