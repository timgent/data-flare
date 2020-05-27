package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QCCheck, QcType}

private [sparkdataquality] trait MetricsBasedCheck extends QCCheck {

  override def qcType: QcType = QcType.MetricsBasedQualityCheck

  protected final def metricTypeErrorResult: CheckResult =
    CheckResult(qcType, CheckStatus.Error,
      "Found metric of the wrong type for this check. Please report this error - this should not occur", description)
  protected final def metricNotPresentErrorResult: CheckResult =
    CheckResult(qcType, CheckStatus.Error,
      "Failed to find corresponding metric for this check. Please report this error - this should not occur", description)
}
