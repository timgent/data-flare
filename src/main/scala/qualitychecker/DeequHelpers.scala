package qualitychecker

import java.time.Instant

import com.amazon.deequ.VerificationResult
import qualitychecker.CheckResultDetails.DeequCheckSuiteResultDetails
import qualitychecker.checks.QCCheck.DeequQCCheck
import qualitychecker.checks.{CheckResult, CheckStatus}

object DeequHelpers {
  implicit class VerificationResultToQualityCheckResult(verificationResult: VerificationResult) {
    def toCheckSuiteResult(description: String, timestamp: Instant): ChecksSuiteResult[DeequCheckSuiteResultDetails] = {
      val checkStatus = verificationResult.status match {
        case com.amazon.deequ.checks.CheckStatus.Success => CheckSuiteStatus.Success
        case com.amazon.deequ.checks.CheckStatus.Warning => CheckSuiteStatus.Warning
        case com.amazon.deequ.checks.CheckStatus.Error => CheckSuiteStatus.Error
      }
      val checkSuiteResultDescription = checkStatus match {
        case qualitychecker.CheckSuiteStatus.Success => "All Deequ checks were successful"
        case qualitychecker.CheckSuiteStatus.Warning => "Deequ checks returned a warning"
        case CheckSuiteStatus.Error => "Deequ checks returned an error"
      }
      val checkResults = verificationResult.checkResults.map{ case (deequCheck, deequCheckResult) =>
        val checkResultDescription = deequCheckResult.status match {
          case com.amazon.deequ.checks.CheckStatus.Success => "Deequ check was successful"
          case com.amazon.deequ.checks.CheckStatus.Warning => "Deequ check produced a warning"
          case com.amazon.deequ.checks.CheckStatus.Error => "Deequ check produced an error"
        }
        CheckResult(deequCheckResult.status.toCheckStatus, checkResultDescription, DeequQCCheck(deequCheck))
      }.toSeq
      ChecksSuiteResult( // Do we want to add deequ constraint results to the checks suite result too? It's another level compared to what we have elsewhere. Could refactor to match deequ's way of doing things
        checkStatus,
        description,
        checkSuiteResultDescription,
        checkResults,
        timestamp,
        QcType.DeequQualityCheck,
        Map.empty,
        DeequCheckSuiteResultDetails(verificationResult.checkResults)
      )
    }
  }

  implicit class DeequCheckStatusEnricher(checkStatus: DeequCheckStatus) {
    def toCheckStatus = checkStatus match {
      case DeequCheckStatus.Success => CheckStatus.Success
      case DeequCheckStatus.Warning => CheckStatus.Warning
      case DeequCheckStatus.Error => CheckStatus.Error
    }
  }
}
