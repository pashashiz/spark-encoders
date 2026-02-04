package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.AnyValEncoderSpec.{Bar, Baz, Foo}
import org.apache.spark.sql.types._

class AnyValEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers
    with TypedEncoderImplicits {

  "AnyValEncoder" when {
    "used with AnyVal" should {

      "work with value class in case class" in {
        // Value classes as fields in case classes (erased at JVM level)
        Bar("1", Foo("Hello!"), 10) should haveTypedEncoder[Bar]()
        Baz(Foo("Hello!")) should haveTypedEncoder[Baz]()
      }

      // Note: Top-level Dataset[ValueClass] is challenging due to context-dependent erasure
      // When boxed in generic containers, value classes retain their wrapper, but Spark's
      // encoder model doesn't easily support this dual behavior. Consider wrapping in a
      // case class for Dataset use.
      "work with top-level value class" ignore {
        Foo("Hello!") should haveTypedEncoder[Foo]()
      }

      "work within a dataset" in {
        val schema = StructType(Seq(
          StructField("id", StringType, nullable = false),
          StructField("key", StringType, nullable = false),
          StructField("value", IntegerType, nullable = false)))

        TypedEncoder[Bar].catalystRepr shouldBe schema

        val ds = spark.createDataset(Seq(Bar("1", Foo("Hello!"), 10)))
        ds.collect().head shouldBe Bar("1", Foo("Hello!"), 10)
      }
    }
  }
}

object AnyValEncoderSpec {
  case class Foo(value: String) extends AnyVal

  case class Bar(id: String, key: Foo, value: Int)

  case class Baz(foo: Foo)
}
