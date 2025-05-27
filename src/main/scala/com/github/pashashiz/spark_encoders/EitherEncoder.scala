package com.github.pashashiz.spark_encoders

import java.io.Serializable

sealed trait EitherStruct[+A, +B] extends Product with Serializable {
  def toEither: Either[A, B]
}

object EitherStruct {
  case class LeftSide[+A, +B](left: A) extends EitherStruct[A, B] {
    override def toEither: Either[A, B] = Left(left)
  }
  case class RightSide[+A, +B](right: B) extends EitherStruct[A, B] {
    override def toEither: Either[A, B] = Right(right)
  }
  def fromEither[A, B](either: Either[A, B]): EitherStruct[A, B] =
    either match {
      case Left(value)  => LeftSide(value)
      case Right(value) => RightSide(value)
    }
}

class EitherInvariant[A, B] extends Invariant[Either[A, B], EitherStruct[A, B]] {
  override def map(in: Either[A, B]): EitherStruct[A, B] = EitherStruct.fromEither(in)
  override def contrMap(out: EitherStruct[A, B]): Either[A, B] = out.toEither
  override def toString = s"EitherInvariant"
}
