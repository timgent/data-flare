package qualitychecker

import java.time.Instant

object QualityChecker {
  def doQualityChecks(qualityChecks: Seq[QualityChecks[_]],
                      metricsRepository: QcResultsRepository,
                      timestamp: Instant): Seq[QualityCheckResult[_]] = {
    val qualityCheckResults: Seq[QualityCheckResult[_]] = qualityChecks.map(_.run(timestamp))
    metricsRepository.save(qualityCheckResults)
    qualityCheckResults
  }
}
