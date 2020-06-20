package com.github.timgent.sparkdataquality.checkssuite

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}

object ChecksSuiteResultStatusCalculator {

  /**
    * Gets the worst status of the given checkResults
    * @param checkResults
    * @return
    */
  def getWorstCheckStatus(checkResults: Seq[CheckResult]): CheckSuiteStatus = {
    checkResults.map(_.status).foldLeft[CheckSuiteStatus](CheckSuiteStatus.Success) {
      case (_, CheckStatus.Error)   => return CheckSuiteStatus.Error
      case (_, CheckStatus.Warning) => CheckSuiteStatus.Warning
      case (_, CheckStatus.Success) => CheckSuiteStatus.Success
    }
  }
}
