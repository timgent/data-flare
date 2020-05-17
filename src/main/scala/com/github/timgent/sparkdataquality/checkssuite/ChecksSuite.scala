package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import enumeratum._

import scala.concurrent.{ExecutionContext, Future}


trait ChecksSuite {
  def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult]

  def checkSuiteDescription: String

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

sealed trait QcType extends EnumEntry

object QcType extends Enum[QcType] {
  val values = findValues
  case object DeequQualityCheck extends QcType
  case object SingleDatasetQualityCheck extends QcType
  case object DatasetComparisonQualityCheck extends QcType
  case object ArbitraryQualityCheck extends QcType
  case object MetricsBasedQualityCheck extends QcType
}