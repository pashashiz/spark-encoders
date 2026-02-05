package io.github.pashashiz.spark_encoders
import NestedObjectSpec._

class NestedObjectSpec extends SparkAnyWordSpec() with TypedEncoderMatchers
    with TypedEncoderImplicits {

  "NestedObject" when {

    "used with complex types" should {

      "work correctly" in {
        Foo(
          Some("Hello"),
          (
            NestedObject(
              Some("Hello!")),
            Some(DoubleNext(1.5)))) should haveTypedEncoder[Foo]()
      }

      "work correctly with only nullable nested objects" in {
        Foo2(
          Some("Hello"),
          Some(NestedTwo(
            Some(NestedObject(
              Some("Hello!"))),
            Some(DoubleNext(1.5)))))   should haveTypedEncoder[Foo2]()
      }
    }
  }
}

object NestedObjectSpec {

  case class NestedObject(value: Option[String])
  case class DoubleNext(value: Double)
  case class Foo(str: Option[String], nestedObject: (NestedObject, Option[DoubleNext]))

  case class NestedTwo(obj: Option[NestedObject], doubleNezt: Option[DoubleNext])
  case class Foo2(str: Option[String], nestedObject: Option[NestedTwo])
}
