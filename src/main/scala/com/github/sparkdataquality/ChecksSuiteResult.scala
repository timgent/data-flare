package com.github.sparkdataquality

import java.time.Instant

import enumeratum._
import com.github.sparkdataquality.checks.CheckResult

case class ChecksSuiteResult(
                                                       overallStatus: CheckSuiteStatus,
                                                       checkSuiteDescription: String,
                                                       resultDescription: String,
                                                       checkResults: Seq[CheckResult],
                                                       timestamp: Instant,
                                                       checkType: QcType,
                                                       checkTags: Map[String, String]
                                                      ) {
  def removeDetails: ChecksSuiteResult =
    ChecksSuiteResult(overallStatus, checkSuiteDescription, resultDescription, checkResults, timestamp, checkType, checkTags)
}

case class SimpleQualityCheckResult()

sealed trait CheckSuiteStatus extends EnumEntry

object CheckSuiteStatus extends Enum[CheckSuiteStatus] {
  val values = findValues
  case object Success extends CheckSuiteStatus
  case object Warning extends CheckSuiteStatus
  case object Error extends CheckSuiteStatus
}
