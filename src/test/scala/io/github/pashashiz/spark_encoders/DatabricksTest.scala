package io.github.pashashiz.spark_encoders

/** This test showcases how to validate the encoders on Databricks.
 *  Call it like this: new io.github.pashashiz.spark_encoders.DatabricksTest().execute()
 */
class DatabricksTest extends SparkAnyWordSpec() with TypedEncoderMatchers with SampleEncoders
  with TypedEncoderImplicits {

  "TypedEncoder" when {
    "used with a complex type" should {
      "work" in {
        testutils.ComplexWrapper.defaultValue should haveTypedEncoder[testutils.ComplexWrapper]()
      }
    }
  }
}
