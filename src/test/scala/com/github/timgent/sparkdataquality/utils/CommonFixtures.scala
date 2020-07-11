package com.github.timgent.sparkdataquality.utils

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.CheckStatus.Success
import com.github.timgent.sparkdataquality.checks.{RawCheckResult, ArbSingleDsCheck}
import com.github.timgent.sparkdataquality.metrics.ComplianceFn
import org.apache.spark.sql.functions.col

object CommonFixtures {
  val now = Instant.now
  val later = now.plusSeconds(10)
  val someCheck = ArbSingleDsCheck("some check")(_ => RawCheckResult(Success, "successful"))
  val someTags = Map("project" -> "project A")
  val datasourceDescription = "datasetDescription"
  val someComplianceFn = ComplianceFn(col("someColumn"), "someComplianceFn")

  case class NumberString(number: Int, str: String)
}
