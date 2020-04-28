package utils

import java.time.Instant

import qualitychecker.checks.QCCheck.SingleDatasetCheck
import qualitychecker.checks.{CheckStatus, RawCheckResult}

object CommonFixtures {
  val now = Instant.now
  val someCheck = SingleDatasetCheck("some check")(_ => RawCheckResult(CheckStatus.Success, "successful"))

}
