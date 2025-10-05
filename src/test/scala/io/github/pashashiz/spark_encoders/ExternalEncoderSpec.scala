package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.Encoders

class ExternalEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers {
  import ExternalEncoderSpec._

  "ExternalEncoder" when {

    "used with a type for which there's a custom encoder" should {

      "work" in {
        // note, this works only using codegen, in interpreted mode it fails since
        // JavaSerializerInstance is not serializable
        new SimpleType(true) should haveTypedEncoder[SimpleType](interpreted = false)
      }
    }
  }
}

object ExternalEncoderSpec {

  class SimpleType(val booleanField: Boolean) extends Serializable {
    override def equals(obj: Any): Boolean = obj match {
      case other: SimpleType => other.booleanField == booleanField
      case _                 => false
    }
    override def hashCode(): Int = booleanField.hashCode()
  }

  object SimpleType {
    implicit val encoder: TypedEncoder[SimpleType] =
      ExternalEncoder(Encoders.javaSerialization[SimpleType])
  }
}
