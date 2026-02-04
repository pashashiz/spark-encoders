package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.AnyValEncoderSpec._
import org.apache.spark.sql.types._

class AnyValEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers
    with TypedEncoderImplicits {

  "AnyValEncoder" when {
    "used with AnyVal" should {

      "work with value class in case class" in {
        implicit val simpleTypeEncoder: TypedEncoder[Foo] =
          TypedEncoder.xmap[Foo, String] { s =>
            s.value
          }(Foo.apply)


        // Value classes as fields in case classes (erased at JVM level)
        Bar("1", Foo("Hello!"), 10) should haveTypedEncoder[Bar]()
        Baz(Foo("Hello!")) should haveTypedEncoder[Baz]()
      }

      "preserve non-anyval" in {
        implicit val simpleTypeEncoder: TypedEncoder[NonAnyVal] =
          TypedEncoder.xmap[NonAnyVal, String] { s =>
            s.value
          }(NonAnyVal.apply)

        NonAnyVal("Hello!") should haveTypedEncoder[NonAnyVal]()
      }
    }
  }
}

object AnyValEncoderSpec {
  case class Foo(value: String) extends AnyVal

  case class Bar(id: String, key: Foo, value: Int)

  case class Baz(foo: Foo)

  case class SimpleTypeStr(value: String) extends AnyVal

  case class ContainedSimple(simple: SimpleTypeStr, bar: Int)

  case class NonAnyVal(value: String)
}
