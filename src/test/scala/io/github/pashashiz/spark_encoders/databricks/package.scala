package io.github.pashashiz.spark_encoders

import org.scalatest.{Args, Suite}
import org.scalatest.tools.PublicStandardOutReporter

package object databricks {
  def runTestsWithAssertions(): Unit = {
    val failedTestSuites = runTests().filterNot(_._2)
    if (failedTestSuites.nonEmpty) {
      val suiteNames = failedTestSuites.map(_._1).map(name => s"'${name}'").mkString(", ")
      val message = s"[FAILURE] The following test suites failed: ${suiteNames}"
      throw new RuntimeException(message)
    } else {
      println("[SUCCESS] All tests passed!")
    }
  }

  def runTests(): Seq[(String, Boolean)] = {
    val reporter = PublicStandardOutReporter(
      presentFullStackTraces = true,
      presentReminderWithFullStackTraces = true,
    )
    suitesToRun.map(suite => suite.suiteName -> suite.run(None, Args(reporter)).succeeds())
  }

  /**
   * We use explicit class names instead of classpath scanning because:
   * 1. Databricks uses custom classloaders (TranslatingClassLoader) that don't expose URLs
   * 2. Notebook environments may have restricted filesystem access for security  
   * 3. The actual classpath includes hundreds of Databricks infrastructure JARs making scanning slow
   */
  val suitesToRun: Seq[Suite] = Seq(
    new TypedEncoderSpec(),
    new InvariantEncoderSpec(),
    new ExternalEncoderSpec(),
  )
}
