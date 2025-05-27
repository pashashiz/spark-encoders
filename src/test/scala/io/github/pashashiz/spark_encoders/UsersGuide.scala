package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.TypedEncoder._

case class User(name: String, age: Int)

sealed trait Item {
  def name: String
}
object Item {
  case class Defect(name: String, priority: Int) extends Item
  case class Feature(name: String, size: Int) extends Item
  case class Story(name: String, points: Int) extends Item
}

sealed trait Color
object Color {
  case object Red extends Color
  case object Green extends Color
  case object Blue extends Color
}

case class Altitude(value: Double)

object UsersGuide {
  def main(args: Array[String]): Unit = {
    val spark = LocalSpark().create
    spark.createDataset(Seq(User("Pavlo", 35), User("Randy", 45))).show(false)
    spark.createDataset[Item](
      Seq(Item.Defect("d1", 100), Item.Feature("f1", 1), Item.Story("s1", 3)))
      .show(false)
    spark.createDataset[Color](
      Seq(Color.Red, Color.Green, Color.Blue))
      .show(false)
    spark.createDataset[Either[String, User]](
      Seq(Right(User("Pavlo", 35)), Left("Oops")))
      .show()
    implicit val altTE: TypedEncoder[Altitude] = xmap[Altitude, Double](_.value)(Altitude.apply)
    spark.createDataset(Seq(Altitude(100), Altitude(132))).show(false)
    spark.close()
  }
}
