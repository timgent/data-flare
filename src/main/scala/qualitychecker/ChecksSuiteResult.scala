package qualitychecker

import java.time.Instant

import enumeratum._
import qualitychecker.CheckResultDetails.{NoDetails, NoDetailsT}
import qualitychecker.checks.CheckResult

case class ChecksSuiteResult[T <: CheckResultDetails](
                                                       overallStatus: CheckSuiteStatus,
                                                       checkSuiteDescription: String,
                                                       resultDescription: String,
                                                       checkResults: Seq[CheckResult],
                                                       timestamp: Instant,
                                                       checkType: QcType,
                                                       checkTags: Map[String, String],
                                                       checkDetails: T // Currently only time it isn't "NoDetails" is for Deequ checks
                                                      ) {
  def removeDetails: ChecksSuiteResult[NoDetailsT] =
    ChecksSuiteResult(overallStatus, checkSuiteDescription, resultDescription, checkResults, timestamp, checkType, checkTags, NoDetails)
}

case class SimpleQualityCheckResult()

trait CheckResultDetails

object CheckResultDetails {

  case class DeequCheckSuiteResultDetails(checkResults: Map[DeequCheck, DeequCheckResult]) extends CheckResultDetails

  case object NoDetails extends CheckResultDetails

  type NoDetailsT = NoDetails.type
}

sealed trait CheckSuiteStatus extends EnumEntry

object CheckSuiteStatus extends Enum[CheckSuiteStatus] {
  val values = findValues
  case object Success extends CheckSuiteStatus
  case object Warning extends CheckSuiteStatus
  case object Error extends CheckSuiteStatus
}
