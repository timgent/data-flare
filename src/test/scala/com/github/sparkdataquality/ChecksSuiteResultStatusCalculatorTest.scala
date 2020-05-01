package com.github.sparkdataquality

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.github.sparkdataquality.ChecksSuiteResultStatusCalculator.getWorstCheckStatus
import com.github.sparkdataquality.checks.{CheckResult, CheckStatus}
import com.github.sparkdataquality.utils.CommonFixtures._

class ChecksSuiteResultStatusCalculatorTest extends AnyWordSpec with Matchers {
  "getWorstCheckStatus" should {
    val successfulCheckResult = CheckResult(CheckStatus.Success, "", someCheck.description)
    val errorCheckResult = CheckResult(CheckStatus.Error, "", someCheck.description)
    val warningCheckResult = CheckResult(CheckStatus.Warning, "", someCheck.description)
    "Choose error status when there is at least one error" in {
      getWorstCheckStatus(List(successfulCheckResult, errorCheckResult, warningCheckResult)) shouldBe CheckSuiteStatus.Error
    }
    "Choose warning status when there is at least one warning and no errors" in {
      getWorstCheckStatus(List(successfulCheckResult, warningCheckResult)) shouldBe CheckSuiteStatus.Warning
    }
    "Choose success status when all checks were successful" in {
      getWorstCheckStatus(List(successfulCheckResult)) shouldBe CheckSuiteStatus.Success
    }
    "Choose success status when there were no checks" in {
      getWorstCheckStatus(List()) shouldBe CheckSuiteStatus.Success
    }
  }
}
