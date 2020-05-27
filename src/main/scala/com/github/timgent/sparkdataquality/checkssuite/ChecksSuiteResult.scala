package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.CheckResult
import enumeratum._

/**
 *
 * @param overallStatus - Overall status of the CheckSuite. Dependent on the checks within the check suite
 * @param checkSuiteDescription - Description of the suite of checks that was run
 * @param resultDescription - Overall description of the result of the CheckSuite
 * @param checkResults - Sequence of CheckResult for every check in the CheckSuite
 * @param timestamp - the time the checks were run
 * @param checkType - the type of the CheckSuite that was run
 * @param checkTags - any tags associated with the CheckSuite
 */
case class ChecksSuiteResult(
                                                       overallStatus: CheckSuiteStatus,
                                                       checkSuiteDescription: String,
                                                       resultDescription: String,
                                                       checkResults: Seq[CheckResult],
                                                       timestamp: Instant,
                                                       checkTags: Map[String, String]
                                                      )

/**
 * Represents the overall status of a CheckSuite
 */
sealed trait CheckSuiteStatus extends EnumEntry

object CheckSuiteStatus extends Enum[CheckSuiteStatus] {
  val values = findValues
  case object Success extends CheckSuiteStatus
  case object Warning extends CheckSuiteStatus
  case object Error extends CheckSuiteStatus
}

case class ChecksSuitesResults(results: Seq[ChecksSuiteResult])