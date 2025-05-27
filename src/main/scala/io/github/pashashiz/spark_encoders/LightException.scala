package io.github.pashashiz.spark_encoders

case class LightException(message: String)
    extends RuntimeException(message, null, true, false)
