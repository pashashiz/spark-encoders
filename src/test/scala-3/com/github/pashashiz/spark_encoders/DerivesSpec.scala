package com.github.pashashiz.spark_encoders

import org.apache.spark.sql.types._

import scala.collection.immutable

case class SimpleUser2(name: String, age: Int) derives TypedEncoder

sealed trait WorkItem2 derives TypedEncoder {
  def name: String
}
object WorkItem2 {
  case class Defect(name: String, priority: Int) extends WorkItem2
  case class Feature(name: String, size: Int) extends WorkItem2
  case class Story(name: String, points: Int) extends WorkItem2
}

class DerivesSpec extends SparkAnyWordSpec() with TypedEncoderMatchers  {

  "TypedEncoder" when {

    "used with product type" should {

      "support all required fields" in {
        val schema = StructType(Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)))
        TypedEncoder[SimpleUser2].catalystRepr shouldBe schema
        SimpleUser2("Pablo", 34) should haveTypedEncoder[SimpleUser2]()
      }
    }

    "used with ADT type" should {

      "support sub types with 1 same field and 1 different" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("points", IntegerType, nullable = true),
          StructField("priority", IntegerType, nullable = true),
          StructField("size", IntegerType, nullable = true)))
        TypedEncoder[WorkItem2].catalystRepr shouldBe schema
        (WorkItem2.Defect("My defect", 10): WorkItem2) should haveTypedEncoder[WorkItem2]()
        (WorkItem2.Story("My story", 5): WorkItem2) should haveTypedEncoder[WorkItem2]()
        (WorkItem2.Feature("My feature", 1): WorkItem2) should haveTypedEncoder[WorkItem2]()
      }
    }
  }
}
