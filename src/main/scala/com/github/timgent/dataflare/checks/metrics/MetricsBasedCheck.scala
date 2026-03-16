package com.github.timgent.dataflare.checks.metrics

import com.github.timgent.dataflare.FlareError
import com.github.timgent.dataflare.FlareError.{LookedUpMetricOfWrongType, MetricCalculationError, MetricLookupError, MetricMissing}
import com.github.timgent.dataflare.checks.{CheckResult, CheckStatus, DatasourceDescription, QCCheck, QcType}
import com.github.timgent.dataflare.metrics.{MetricDescriptor, MetricValue}

import scala.reflect.ClassTag

private[dataflare] trait MetricsBasedCheck extends QCCheck {

  // typeTag required here to enable match of metric on type MV. Without class tag this type check would be fruitless
  protected def getMetric[MV <: MetricValue](metricDescriptor: MetricDescriptor.Aux[MV], metrics: Map[MetricDescriptor, MetricValue])(
      implicit classTag: ClassTag[MV]
  ): Either[MetricLookupError, MV] = {
    val metricOfInterestOpt: Option[MetricValue] =
      metrics.get(metricDescriptor).map(metricValue => metricValue)
    metricOfInterestOpt match {
      case Some(metric) =>
        metric match { // TODO: Look into heterogenous maps to avoid this type test - https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-1.2.4#heterogenous-maps
          case metric: MV => Right(metric)
          case _          => Left(LookedUpMetricOfWrongType)
        }
      case None => Left(MetricMissing)
    }
  }

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
