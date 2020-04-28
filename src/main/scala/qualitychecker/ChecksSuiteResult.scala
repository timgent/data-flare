package qualitychecker

import java.time.Instant

import qualitychecker.CheckResultDetails.NoDetails
import qualitychecker.checks.CheckResult

case class ChecksSuiteResult[T <: CheckResultDetails](
                                                       overallStatus: CheckSuiteStatus.Value,
                                                       checkSuiteDescription: String,
                                                       resultDescription: String,
                                                       checkResults: Seq[CheckResult],
                                                       timestamp: Instant,
                                                       checkType: QcType.Value,
                                                       checkTags: Map[String, String],
                                                       checkDetails: T // Currently only time it isn't "NoDetails" is for Deequ checks
                                                      ) {
  def removeDetails: ChecksSuiteResult[NoDetails] =
    ChecksSuiteResult(overallStatus, checkSuiteDescription, resultDescription, checkResults, timestamp, checkType, checkTags, NoDetails)
}

case class SimpleQualityCheckResult()

trait CheckResultDetails

object CheckResultDetails {

  case class DeequCheckSuiteResultDetails(checkResults: Map[DeequCheck, DeequCheckResult]) extends CheckResultDetails

  case object NoDetails extends CheckResultDetails

  type NoDetails = NoDetails.type
}

object CheckSuiteStatus extends Enumeration {
  val Success, Warning, Error = Value
}
