package io.github.pashashiz.spark_encoders.testutils

import io.github.pashashiz.spark_encoders.Invariant

import java.time.LocalDate
import java.util.UUID

sealed trait SimpleType

case class SimpleTypeA(
  booleanField: Boolean,
  optionalString: Option[String],
  listOfInts: List[Int],
  mapOfStrings: Map[String, Int]
) extends SimpleType

case class SimpleTypeB(
  intField: Int,
  optionalUuid: Option[UUID],
  vectorOfDates: Vector[LocalDate],
  mapOfOptions: Map[String, Option[Boolean]]
) extends SimpleType

case object SimpleTypeC extends SimpleType

object SimpleTypeA {
  val defaultValue: SimpleTypeA = SimpleTypeA(
    booleanField = true,
    optionalString = Some("default"),
    listOfInts = List(1, 2, 3),
    mapOfStrings = Map("key1" -> 10, "key2" -> 20)
  )
}

object SimpleTypeB {
  val defaultValue: SimpleTypeB = SimpleTypeB(
    intField = 42,
    optionalUuid = Some(UUID.fromString("550e8400-e29b-41d4-a716-446655440000")),
    vectorOfDates = Vector(LocalDate.of(2023, 1, 1), LocalDate.of(2023, 12, 31)),
    mapOfOptions = Map("enabled" -> Some(true), "disabled" -> None)
  )
}

object SimpleType {
  val defaultValue: SimpleType = SimpleTypeA.defaultValue
}

class SimpleTypeWithCustomEncoder(
  val toCaseClass: SimpleType
) extends Invariant[SimpleTypeWithCustomEncoder, SimpleType] {
  override def map(in: SimpleTypeWithCustomEncoder): SimpleType = in.toCaseClass

  override def contrMap(out: SimpleType): SimpleTypeWithCustomEncoder =
    new SimpleTypeWithCustomEncoder(out)
}
