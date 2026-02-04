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

      "work with top-level value class" in {
        // Top-level Dataset[Foo] should work - the schema is the underlying type (String)
        TypedEncoder[Foo].catalystRepr shouldBe StringType

        val ds = spark.createDataset(Seq(Foo("Hello!")))
        ds.schema shouldBe StructType(Seq(StructField("value", StringType, nullable = true)))
        ds.collect().head shouldBe Foo("Hello!")

        // Test that a raw thing would work too
        val ds2 = spark.createDataset(Seq("Hello!")).as[Foo]
        ds2.collect().head shouldBe Foo("Hello!")

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
