package com.github.timgent.dataflare.checkssuite

import java.time.Instant

import com.github.timgent.dataflare.checks.{CheckResult, CheckStatus}
import enumeratum._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Defines a suite of checks to be done
  */
trait ChecksSuiteBase {

  /**
    * Run all checks in the ChecksSuite
    * @param timestamp - time the checks are being run
    * @param ec - execution context
    * @return
    */
  def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult]

  /**
    * Description of the check suite
    * @return
    */
  def checkSuiteDescription: String
}
