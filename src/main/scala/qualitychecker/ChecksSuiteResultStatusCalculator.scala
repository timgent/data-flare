package qualitychecker

import qualitychecker.checks.{CheckResult, CheckStatus}

object ChecksSuiteResultStatusCalculator {
  def getWorstCheckStatus(checkResults: Seq[CheckResult]): CheckSuiteStatus = {
      checkResults.map(_.status).foldLeft[CheckSuiteStatus](CheckSuiteStatus.Success){
        case (_, CheckStatus.Error) => return CheckSuiteStatus.Error
        case (_, CheckStatus.Warning) => CheckSuiteStatus.Warning
        case (_, CheckStatus.Success) => CheckSuiteStatus.Success
      }
  }
}
