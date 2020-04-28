package qualitychecker

import qualitychecker.checks.{CheckResult, CheckStatus}

object ChecksSuiteResultStatusCalculator {
  def getWorstCheckStatus(checkResults: Seq[CheckResult]): CheckSuiteStatus.Value = {
      checkResults.map(_.status).foldLeft(CheckSuiteStatus.Success){
        case (_, CheckStatus.Error) => return CheckSuiteStatus.Error
        case (_, CheckStatus.Warning) => CheckSuiteStatus.Warning
        case (_, CheckStatus.Success) => CheckSuiteStatus.Success
      }
  }
}
