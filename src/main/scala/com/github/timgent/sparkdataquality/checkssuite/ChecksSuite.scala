package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import enumeratum._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Defines a suite of checks to be done
 */
trait ChecksSuite {
  /**
   * Run all checks in the ChecksSuite
   * @param timestamp - time the checks are being run
   * @param ec - execution context
   * @return
   */
  def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult]

  /**
   * Description of the check suite
   * @return
   */
  def checkSuiteDescription: String

  /**
   * The type of the check suite
   * @return
   */
  def qcType: QcType
}

object ChecksSuite {
  private[checkssuite] def getOverallCheckResultDescription(checkResults: Seq[CheckResult]): String = {
    val successfulCheckCount = checkResults.count(_.status == CheckStatus.Success)
    val erroringCheckCount = checkResults.count(_.status == CheckStatus.Error)
    val warningCheckCount = checkResults.count(_.status == CheckStatus.Warning)
    s"$successfulCheckCount checks were successful. $erroringCheckCount checks gave errors. $warningCheckCount checks gave warnings"
  }
}

private [sparkdataquality] sealed trait QcType extends EnumEntry

private [sparkdataquality] object QcType extends Enum[QcType] {
  val values = findValues
  case object DeequQualityCheck extends QcType
  case object SingleDatasetQualityCheck extends QcType
  case object DatasetComparisonQualityCheck extends QcType
  case object ArbitraryQualityCheck extends QcType
  case object MetricsBasedQualityCheck extends QcType
}