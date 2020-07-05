package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.DatasourceDescription.DualDsDescription
import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.sparkdataquality.metrics.{MetricComparator, MetricDescriptor, MetricValue}

import scala.reflect.ClassTag

/**
  * A check based on a metric for one dataset compared to a metric on another dataset
  *
  * @param dsAMetric - the metric to be used on dataset a
  * @param dsBMetric - the metric to be used on dataset b
  * @param metricComparator    - comparison function for the metrics which determines if the check passes
  * @param userDescription     - description of the check
  * @tparam MV
  */
final case class DualMetricBasedCheck[MV <: MetricValue](
    dsAMetric: MetricDescriptor,
    dsBMetric: MetricDescriptor,
    userDescription: String,
    metricComparator: MetricComparator[MV])
    extends MetricsBasedCheck {

  override def description: String =
    s"$userDescription. Comparing metric ${dsAMetric.toSimpleMetricDescriptor} to ${dsBMetric} using comparator of ${metricComparator.description}"

  override def qcType: QcType = QcType.MetricsBasedQualityCheck

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
    val dsAMetricOpt: Option[MetricValue] = dsAMetrics.get(dsAMetric)
    val dsBMetricOpt: Option[MetricValue] = dsBMetrics.get(dsBMetric)
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
