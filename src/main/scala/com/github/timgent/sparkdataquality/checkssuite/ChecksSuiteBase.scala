package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import enumeratum._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Defines a suite of checks to be done
  */
trait ChecksSuiteBase {

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
}

object ChecksSuiteBase {
  private[checkssuite] def getOverallCheckResultDescription(
      checkResults: Seq[CheckResult]
  ): String = {
    val successfulCheckCount = checkResults.count(_.status == CheckStatus.Success)
    val erroringCheckCount = checkResults.count(_.status == CheckStatus.Error)
    val warningCheckCount = checkResults.count(_.status == CheckStatus.Warning)
    s"$successfulCheckCount checks were successful. $erroringCheckCount checks gave errors. $warningCheckCount checks gave warnings"
  }
}
