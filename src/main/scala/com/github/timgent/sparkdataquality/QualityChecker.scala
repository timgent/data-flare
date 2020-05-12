package com.github.timgent.sparkdataquality

import java.time.Instant

import com.github.timgent.sparkdataquality.checkssuite.{ChecksSuite, ChecksSuiteResult, ChecksSuitesResults}
import com.github.timgent.sparkdataquality.repository.QcResultsRepository

object QualityChecker {
  def doQualityChecks(qualityChecks: Seq[ChecksSuite],
                      metricsRepository: QcResultsRepository,
                      timestamp: Instant): ChecksSuitesResults = {
    val qualityCheckResults: Seq[ChecksSuiteResult] = qualityChecks.map(_.run(timestamp))
    metricsRepository.save(qualityCheckResults)
    ChecksSuitesResults(qualityCheckResults)
  }
}
