package qualitychecker

import java.time.Instant

import com.amazon.deequ.VerificationResult
import qualitychecker.CheckResultDetails.DeequCheckResultDetails

object DeequHelpers {
  implicit class VerificationResultToQualityCheckResult(verificationResult: VerificationResult) {
    def toQualityCheckResult(description: String, timestamp: Instant): QualityCheckResult[DeequCheckResultDetails] = {
      val checkStatus = verificationResult.status match {
        case com.amazon.deequ.checks.CheckStatus.Success => CheckStatus.Success
        case com.amazon.deequ.checks.CheckStatus.Warning => CheckStatus.Warning
        case com.amazon.deequ.checks.CheckStatus.Error => CheckStatus.Error
      }
      QualityCheckResult(
        checkStatus,
        description,
        "deequChecks failed", // TODO: Give proper result description for deequCheck
        Seq.empty, // TODO: Populate constraint results properly for deequ checks
        timestamp,
        QcType.DeequQualityCheck,
        Map.empty,
        DeequCheckResultDetails(verificationResult.checkResults)
      )
    }
  }
}
