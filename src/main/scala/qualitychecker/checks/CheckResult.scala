package qualitychecker.checks

case class RawCheckResult(status: CheckStatus.Value, resultDescription: String) {
  def toCheckResult(constraint: QCCheck) = CheckResult(status, resultDescription, constraint)
}

case class CheckResult(
                        status: CheckStatus.Value,
                        resultDescription: String,
                        check: QCCheck
                      )
