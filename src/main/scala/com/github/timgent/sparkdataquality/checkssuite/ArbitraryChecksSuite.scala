package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.CheckResult
import com.github.timgent.sparkdataquality.checks.QCCheck.ArbitraryCheck
import com.github.timgent.sparkdataquality.checkssuite.ChecksSuite.getOverallCheckResultDescription

trait ArbitraryChecksSuite extends ChecksSuite

object ArbitraryChecksSuite {
  def apply(checkDesc: String,
            checks: Seq[ArbitraryCheck],
            checkTags: Map[String, String],
            checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
           ): ArbitraryChecksSuite =
    new ArbitraryChecksSuite {
      override def run(timestamp: Instant): ChecksSuiteResult = {
        val checkResults: Seq[CheckResult] = checks.map(_.applyCheck)
        val overallCheckStatus = checkResultCombiner(checkResults)
        ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
          checkResults, timestamp, qcType, checkTags)
      }

      override def checkSuiteDescription: String = checkDesc

      override def qcType: QcType = QcType.ArbitraryQualityCheck
    }
}