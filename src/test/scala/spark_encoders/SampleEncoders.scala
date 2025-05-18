package spark_encoders

import spark_encoders.TypedEncoder._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, SQLUserDefinedType, UserDefinedType}

case class Container[A](value: A)

case class SimpleUser(name: String, age: Int)
case class UserOptAge(name: String, age: Option[Int])
case class UserOptName(name: Option[String], age: Int)
case class UserOptBoth(name: Option[String], age: Option[Int])

case class SimpleTask(name: String, user: SimpleUser)
case class SimpleTaskOptUser(name: String, user: Option[SimpleUser])

sealed trait WorkItem {
  def name: String
}

object WorkItem {
  case class Defect(name: String, priority: Int) extends WorkItem
  case class Feature(name: String, size: Int) extends WorkItem
  case class Story(name: String, points: Int) extends WorkItem
}

sealed trait WorkItemOpt
object WorkItemOpt {
  case class Defect(name: Option[String], priority: Int) extends WorkItemOpt
  case class Feature(name: Option[String], size: Int) extends WorkItemOpt
}

sealed trait WorkItemDiffFields
object WorkItemDiffFields {
  case class Defect(name: String, priority: Int) extends WorkItemDiffFields
  case class Feature(description: String, size: Int) extends WorkItemDiffFields
}

sealed trait WorkItemDiffType {
  def name: String
}
object WorkItemDiffType {
  case class Defect(name: String, size: Int) extends WorkItemDiffType
  case class Story(name: String, size: Float) extends WorkItemDiffType
}

sealed trait PR {
  def item: WorkItem
}
object PR {
  case class Draft(item: WorkItem, description: String) extends PR
  case class InProgress(item: WorkItem, description: String, reviewer: SimpleUser) extends PR
}

sealed trait PROpt {
  def item: Option[WorkItem]
}
object PROpt {
  case class Draft(item: Option[WorkItem], description: String) extends PROpt
  case class InProgress(item: Option[WorkItem], description: String, reviewer: Option[SimpleUser])
      extends PROpt
}

sealed trait NestedEnum
object NestedEnum {
  case object Case1 extends NestedEnum
  case object Case2 extends NestedEnum
}

sealed trait FlatEnum
case object FlatEnumCase1 extends FlatEnum
case object FlatEnumCase2 extends FlatEnum

sealed trait UserAttribute
object UserAttribute {
  case class Name(name: String) extends UserAttribute
  case class Age(value: Int) extends UserAttribute
  case object Unknown extends UserAttribute
}

case class Error(message: String)

case class EmptyCaseClass()
case object EmptyCaseObject

case class ExceptionWrapper(value: RuntimeException)

@SQLUserDefinedType(udt = classOf[PointUDT])
case class Point(x: Double, y: Double)

class PointUDT extends UserDefinedType[Point] {
  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)
  override def serialize(point: Point): Any =
    new GenericArrayData(Array[Double](point.x, point.y))
  override def deserialize(datum: Any): Point =
    datum match {
      case values: ArrayData =>
        Point(values.getDouble(0), values.getDouble(1))
    }
  override def userClass: Class[Point] = classOf[Point]
}

trait SampleEncoders {
  implicit def containerEncoder[A: TypedEncoder]: TypedEncoder[Container[A]] =
    genTypedEncoder[Container[A]]
  implicit def simpleUserEncoder: TypedEncoder[SimpleUser] = genTypedEncoder[SimpleUser]
  implicit def simpleUserAsTupleEncoder: TypedEncoder[(String, Int)] =
    genTypedEncoder[(String, Int)]
  implicit def userOptAgeEncoder: TypedEncoder[UserOptAge] = genTypedEncoder[UserOptAge]
  implicit def userOptNameEncoder: TypedEncoder[UserOptName] = genTypedEncoder[UserOptName]
  implicit def userOptBothEncoder: TypedEncoder[UserOptBoth] = genTypedEncoder[UserOptBoth]
  implicit def simpleTaskEncoder: TypedEncoder[SimpleTask] = genTypedEncoder[SimpleTask]
  implicit def simpleTaskOptUserEncoder: TypedEncoder[SimpleTaskOptUser] =
    genTypedEncoder[SimpleTaskOptUser]
  implicit def workItemEncoder: TypedEncoder[WorkItem] = genTypedEncoder[WorkItem]
  implicit def workItemDiffTypeEncoder: TypedEncoder[WorkItemDiffType] =
    genTypedEncoder[WorkItemDiffType]
  implicit def workItemDiffFieldsEncoder: TypedEncoder[WorkItemDiffFields] =
    genTypedEncoder[WorkItemDiffFields]
  implicit def workItemMixedOptEncoder: TypedEncoder[WorkItemOpt] = genTypedEncoder[WorkItemOpt]
  implicit def prEncoder: TypedEncoder[PR] = genTypedEncoder[PR]
  implicit def prOptEncoder: TypedEncoder[PROpt] = genTypedEncoder[PROpt]
  implicit def nestedEnumEncoder: TypedEncoder[NestedEnum] = genTypedEncoder[NestedEnum]
  implicit def flatEnumEncoder: TypedEncoder[FlatEnum] = genTypedEncoder[FlatEnum]
  implicit def userAttributeEncoder: TypedEncoder[UserAttribute] =
    genTypedEncoder[UserAttribute]
  implicit def errorEncoder: TypedEncoder[Error] = genTypedEncoder[Error]
  implicit def emptyCaseClassEncoder: TypedEncoder[EmptyCaseClass] = genTypedEncoder[EmptyCaseClass]
  implicit def emptyCaseObjectEncoder: TypedEncoder[EmptyCaseObject.type] =
    genTypedEncoder[EmptyCaseObject.type]
    
  implicit def containerArraySimpleUserEncoder: TypedEncoder[Container[Array[SimpleUser]]] = 
    genTypedEncoder[Container[Array[SimpleUser]]]
  implicit def containerArrayIntEncoder: TypedEncoder[Container[Array[Int]]] =
    genTypedEncoder[Container[Array[Int]]]
  implicit def containerArrayByte: TypedEncoder[Container[Array[Byte]]] =
    genTypedEncoder[Container[Array[Byte]]]
}
