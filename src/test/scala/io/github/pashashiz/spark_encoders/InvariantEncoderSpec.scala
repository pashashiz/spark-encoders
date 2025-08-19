package io.github.pashashiz.spark_encoders

import io.github.pashashiz.spark_encoders.testutils._

class InvariantEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers {

  "ExternalEncoder" when {

    "used with SimpleType" should {

      // TODO: add equality for classes (now eq is based is pointer eq)
      "support SimpleTypeA" in {
        new SimpleTypeWithCustomEncoder(SimpleTypeA(true)) should haveTypedEncoder[SimpleTypeWithCustomEncoder]()
      }
    }
  }
}
