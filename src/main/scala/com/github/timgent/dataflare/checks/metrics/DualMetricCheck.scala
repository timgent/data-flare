package com.github.timgent.dataflare.checks.metrics

import com.github.timgent.dataflare.checks.CheckDescription.DualMetricCheckDescription
import com.github.timgent.dataflare.checks.DatasourceDescription.DualDsDescription
import com.github.timgent.dataflare.checks.QCCheck.DualDsQCCheck
import com.github.timgent.dataflare.checks.{CheckDescription, CheckResult, CheckStatus, QcType}
import com.github.timgent.dataflare.metrics.{MetricComparator, MetricDescriptor, MetricValue}

import scala.reflect.ClassTag

/**
  * A check based on a metric for one dataset compared to a metric on another dataset
  *
  * @param dsMetric - the metric to be used on dataset a
  * @param dsToCompareMetric - the metric to be used on dataset b
  * @param metricComparator    - comparison function for the metrics which determines if the check passes
  * @param checkDescription     - description of the check
  * @tparam MV
  */
final case class DualMetricCheck[MV <: MetricValue](
    dsMetric: MetricDescriptor { type MetricType = MV },
    dsToCompareMetric: MetricDescriptor { type MetricType = MV },
    checkDescription: String,
    metricComparator: MetricComparator[MV]
) extends MetricsBasedCheck
    with DualDsQCCheck {

  override def description: CheckDescription =
    DualMetricCheckDescription(
      checkDescription,
      dsMetric.toSimpleMetricDescriptor,
      dsToCompareMetric.toSimpleMetricDescriptor,
      metricComparator.description
    )

  override def qcType: QcType = QcType.DualMetricCheck

  private def getCheckResult(checkPassed: Boolean, dsAMetric: MV, dsBMetric: MV, dualDsDescription: DualDsDescription): CheckResult = {
    if (checkPassed)
      CheckResult(
        qcType,
        CheckStatus.Success,
        s"metric comparison passed. ${dualDsDescription.datasourceA} with $dsAMetric was compared to ${dualDsDescription.datasourceB} with $dsBMetric",
        description
      )
    else
      CheckResult(
        qcType,
        CheckStatus.Error,
        s"metric comparison failed. ${dualDsDescription.datasourceA} with $dsAMetric was compared to ${dualDsDescription.datasourceB} with $dsBMetric",
        description
      )
  }

  def applyCheckOnMetrics(
      dsAMetrics: Map[MetricDescriptor, MetricValue],
      dsBMetrics: Map[MetricDescriptor, MetricValue],
      dualDsDescription: DualDsDescription
  )(implicit classTag: ClassTag[MV]): CheckResult = {
    val dsAMetricOpt: Option[MetricValue] = dsAMetrics.get(dsMetric)
    val dsBMetricOpt: Option[MetricValue] = dsBMetrics.get(dsToCompareMetric)
    (dsAMetricOpt, dsBMetricOpt) match {
      case (Some(dsAMetric), Some(dsBMetric)) =>
        (dsAMetric, dsBMetric) match {
          case (dsAMetric: MV, dsBMetric: MV) =>
            getCheckResult(metricComparator.fn(dsAMetric.value, dsBMetric.value), dsAMetric, dsBMetric, dualDsDescription)
          case _ => metricTypeErrorResult
        }
      case _ => metricNotPresentErrorResult
    }
  }
}
