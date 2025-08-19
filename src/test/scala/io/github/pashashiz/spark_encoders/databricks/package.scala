package io.github.pashashiz.spark_encoders

import org.scalatest.Args
import org.scalatest.tools.PublicStandardOutReporter

package object databricks {
  def runTests(): Seq[(String, Boolean)] = {
    val reporter = PublicStandardOutReporter(
      presentFullStackTraces = true,
      presentReminderWithFullStackTraces = true,
    )
    specs.map(suite => suite.suiteName -> suite.run(None, Args(reporter)).succeeds())
  }

  // Add new test suites here
  val specs = Seq(
    new TypedEncoderSpec(),
    new InvariantEncoderSpec(),
    new ExternalEncoderSpec(),
  )
}
