package com.github.timgent.sparkdataquality.checks

/**
 * Arbitrary check - could provide any function to do this type of check
 */
trait ArbitraryCheck extends QCCheck {
  def description: String

  def applyCheck: CheckResult
}

object ArbitraryCheck {
  def apply(checkDescription: String)(check: => RawCheckResult): ArbitraryCheck = new ArbitraryCheck {
    override def description: String = checkDescription

    override def applyCheck: CheckResult = check.withDescription(checkDescription)
  }
}