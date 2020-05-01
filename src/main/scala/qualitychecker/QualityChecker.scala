package qualitychecker

import java.time.Instant

import qualitychecker.repository.QcResultsRepository

object QualityChecker {
  def doQualityChecks(qualityChecks: Seq[ChecksSuite],
                      metricsRepository: QcResultsRepository,
                      timestamp: Instant): Seq[ChecksSuiteResult] = {
    val qualityCheckResults: Seq[ChecksSuiteResult] = qualityChecks.map(_.run(timestamp))
    metricsRepository.save(qualityCheckResults)
    qualityCheckResults
  }
}
