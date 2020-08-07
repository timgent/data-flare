package com.github.timgent.dataflare

import com.github.timgent.dataflare.checkssuite.ChecksSuiteResultStatusCalculator.getWorstCheckStatus
import com.github.timgent.dataflare.checks.CheckStatus.Success
import com.github.timgent.dataflare.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.dataflare.checkssuite.CheckSuiteStatus
import com.github.timgent.dataflare.utils.CommonFixtures._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ChecksSuiteResultStatusCalculatorTest extends AnyWordSpec with Matchers {
  "getWorstCheckStatus" should {
    val qcType = QcType.SingleMetricCheck
    val successfulCheckResult = CheckResult(qcType, CheckStatus.Success, "", someCheck.description)
    val errorCheckResult = CheckResult(qcType, CheckStatus.Error, "", someCheck.description)
    val warningCheckResult = CheckResult(qcType, CheckStatus.Warning, "", someCheck.description)
    "Choose error status when there is at least one error" in {
      getWorstCheckStatus(
        List(successfulCheckResult, errorCheckResult, warningCheckResult)
      ) shouldBe CheckSuiteStatus.Error
    }
    "Choose warning status when there is at least one warning and no errors" in {
      getWorstCheckStatus(
        List(successfulCheckResult, warningCheckResult)
      ) shouldBe CheckSuiteStatus.Warning
    }
    "Choose success status when all checks were successful" in {
      getWorstCheckStatus(List(successfulCheckResult)) shouldBe CheckSuiteStatus.Success
    }
    "Choose success status when there were no checks" in {
      getWorstCheckStatus(List()) shouldBe CheckSuiteStatus.Success
    }
  }
}
