package org.scalatest.tools

object PublicStandardOutReporter {
  def apply(
    presentAllDurations: Boolean = false,
    presentInColor: Boolean = false,
    presentShortStackTraces: Boolean = false,
    presentFullStackTraces: Boolean = false,
    presentUnformatted: Boolean = false,
    presentReminder: Boolean = false,
    presentReminderWithShortStackTraces: Boolean = false,
    presentReminderWithFullStackTraces: Boolean = false,
    presentReminderWithoutCanceledTests: Boolean = false,
    presentFilePathname: Boolean = false,
    presentJson: Boolean = false
  ): StandardOutReporter = new StandardOutReporter(
    presentAllDurations,
    presentInColor,
    presentShortStackTraces,
    presentFullStackTraces,
    presentUnformatted,
    presentReminder,
    presentReminderWithShortStackTraces,
    presentReminderWithFullStackTraces,
    presentReminderWithoutCanceledTests,
    presentFilePathname,
    presentJson
  )
}
