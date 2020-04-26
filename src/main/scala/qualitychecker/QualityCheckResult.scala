package qualitychecker

import java.time.Instant

import qualitychecker.CheckResultDetails.NoDetails
import qualitychecker.constraint.ConstraintResult

case class QualityCheckResult[T <: CheckResultDetails](
                                                        overallStatus: CheckStatus.Value,
                                                        checkDescription: String, // TODO: Just include the original check here instead?
                                                        resultDescription: String,
                                                        constraintResults: Seq[ConstraintResult],
                                                        timestamp: Instant,
                                                        checkType: QcType.Value, // TODO: Could remove if we include the original check instead
                                                        checkProperties: Map[String, String], // TODO: Either remove or expose, currently it's always empty
                                                        checkDetails: T // Currently only time it isn't "NoDetails" is for Deequ checks
                                                      ) {
  def removeDetails: QualityCheckResult[NoDetails.type] =
    QualityCheckResult(overallStatus, checkDescription, resultDescription, constraintResults, timestamp, checkType, checkProperties, NoDetails)
}

case class SimpleQualityCheckResult()

trait CheckResultDetails

object CheckResultDetails {

  case class DeequCheckResultDetails(checkResults: Map[DeequCheck, DeequCheckResult]) extends CheckResultDetails

  case object NoDetails extends CheckResultDetails

  type NoDetails = NoDetails.type
}

object CheckStatus extends Enumeration {
  val Success, Warning, Error = Value
}
