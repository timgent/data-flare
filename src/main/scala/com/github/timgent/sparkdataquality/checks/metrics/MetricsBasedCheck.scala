package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QCCheck}

trait MetricsBasedCheck extends QCCheck {
  protected final def metricTypeErrorResult: CheckResult =
    CheckResult(CheckStatus.Error, "Found metric of the wrong type for this check. Please report this error - this should not occur", description)
  protected final def metricNotPresentErrorResult: CheckResult =
    CheckResult(CheckStatus.Error, "Failed to find corresponding metric for this check. Please report this error - this should not occur", description)
}
