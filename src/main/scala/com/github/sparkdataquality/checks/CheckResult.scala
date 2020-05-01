package com.github.sparkdataquality.checks

case class RawCheckResult(
                        status: CheckStatus,
                        resultDescription: String
                      ) {
  def withDescription(checkDescription: String) = CheckResult(status, resultDescription, checkDescription)
}

case class CheckResult(
                        status: CheckStatus,
                        resultDescription: String,
                        checkDescription: String
                      )
