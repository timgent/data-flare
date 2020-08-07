package com.github.timgent.dataflare.utils

import java.time.Instant

import com.github.timgent.dataflare.checks.CheckStatus.Success
import com.github.timgent.dataflare.checks.{RawCheckResult, ArbSingleDsCheck}
import com.github.timgent.dataflare.metrics.ComplianceFn
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
