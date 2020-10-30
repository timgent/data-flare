package com.github.timgent.dataflare.checks.metrics

import com.github.timgent.dataflare.FlareError.MetricCalculationError
import com.github.timgent.dataflare.checks.{CheckResult, CheckStatus, DatasourceDescription, QCCheck, QcType}

private[dataflare] trait MetricsBasedCheck extends QCCheck {

  private[dataflare] def getMetricErrorCheckResult(datasourceDescription: DatasourceDescription, err: MetricCalculationError*) =
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
