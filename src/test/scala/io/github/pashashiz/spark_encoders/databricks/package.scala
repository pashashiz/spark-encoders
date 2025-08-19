package io.github.pashashiz.spark_encoders

import org.scalatest.Args
import org.scalatest.tools.PublicStandardOutReporter

package object databricks {
  def runTests(): Seq[Boolean] = {
    val reporter = PublicStandardOutReporter(presentFullStackTraces = true)
    val specs = Seq(
      new TypedEncoderSpec(),
      new InvariantEncoderSpec(),
      new ExternalEncoderSpec(),
    )
    specs.map(_.run(None, Args(reporter)).succeeds())
  }
}
