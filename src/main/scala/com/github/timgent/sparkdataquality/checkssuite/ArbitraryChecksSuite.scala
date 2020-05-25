package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.{ArbitraryCheck, CheckResult}
import com.github.timgent.sparkdataquality.checkssuite.ChecksSuiteBase.getOverallCheckResultDescription

import scala.concurrent.{ExecutionContext, Future}

/**
 * A Checks Suite for Arbitrary Checks
 */
trait ArbitraryChecksSuite extends ChecksSuiteBase

object ArbitraryChecksSuite {
  def apply(checkDesc: String,
            checks: Seq[ArbitraryCheck],
            checkTags: Map[String, String],
            checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
           ): ArbitraryChecksSuite =
    new ArbitraryChecksSuite {
      override def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult] = {
        val checkResults: Seq[CheckResult] = checks.map(_.applyCheck)
        val overallCheckStatus = checkResultCombiner(checkResults)
        val result = ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
          checkResults, timestamp, checkTags)
        Future.successful(result)
      }

      override def checkSuiteDescription: String = checkDesc
    }
}