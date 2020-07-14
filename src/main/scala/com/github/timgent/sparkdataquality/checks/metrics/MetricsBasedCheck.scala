package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.SdqError.MetricCalculationError
import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, DatasourceDescription, QCCheck, QcType}

private[sparkdataquality] trait MetricsBasedCheck extends QCCheck {

  override def qcType: QcType = QcType.SingleMetricCheck

  private[sparkdataquality] def getMetricErrorCheckResult(datasourceDescription: DatasourceDescription, err: MetricCalculationError*) =
    CheckResult(
      qcType,
      CheckStatus.Error,
      "Check failed due to issue calculating metrics for this dataset",
      description,
      Some(datasourceDescription),
      err
    )

  protected final def metricTypeErrorResult: CheckResult =
    CheckResult(
      qcType,
      CheckStatus.Error,
      "Found metric of the wrong type for this check. Please report this error - this should not occur",
      description
    )
  protected final def metricNotPresentErrorResult: CheckResult =
    CheckResult(
      qcType,
      CheckStatus.Error,
      "Failed to find corresponding metric for this check. Please report this error - this should not occur",
      description
    )
}
