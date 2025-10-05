package io.github.pashashiz.spark_encoders

import org.apache.spark.SparkThrowable

class EncoderException(
    message: String,
    cause: Throwable)
    extends Exception(message, cause) with SparkThrowable {
  def this(message: String) = this(message = message, cause = null)
  override def getErrorClass: String = "ENCODER_ERROR"
  override def getCondition: String = getErrorClass
}
