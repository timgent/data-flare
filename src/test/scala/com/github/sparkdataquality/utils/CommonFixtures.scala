package com.github.sparkdataquality.utils

import java.time.Instant

import com.github.sparkdataquality.checks.QCCheck.SingleDatasetCheck
import com.github.sparkdataquality.checks.{CheckStatus, RawCheckResult}

object CommonFixtures {
  val now = Instant.now
  val someCheck = SingleDatasetCheck("some check")(_ => RawCheckResult(CheckStatus.Success, "successful"))
  val someTags = Map("project" -> "project A")
}
