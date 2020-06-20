package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.sparkdataquality.metrics.{MetricComparator, MetricDescriptor, MetricValue}

import scala.reflect.ClassTag

/**
  * A check based on a metric for one dataset compared to a metric on another dataset
  *
  * @param dsAMetricDescriptor - the metric to be used on dataset a
  * @param dsBMetricDescriptor - the metric to be used on dataset b
  * @param metricComparator    - comparison function for the metrics which determines if the check passes
  * @param userDescription     - description of the check
  * @tparam MV
  */
final case class DualMetricBasedCheck[MV <: MetricValue](
    dsAMetricDescriptor: MetricDescriptor,
    dsBMetricDescriptor: MetricDescriptor,
    userDescription: String
)(metricComparator: MetricComparator[MV])
    extends MetricsBasedCheck {

  override def description: String =
    s"$userDescription. Comparing metric ${dsAMetricDescriptor.toSimpleMetricDescriptor} to ${dsBMetricDescriptor} using comparator of ${metricComparator.description}"

  override def qcType: QcType = QcType.MetricsBasedQualityCheck

  private def getCheckResult(checkPassed: Boolean, dsAMetric: MV, dsBMetric: MV): CheckResult = {
    if (checkPassed)
      CheckResult(
        qcType,
        CheckStatus.Success,
        s"metric comparison passed. dsAMetric of $dsAMetric was compared to dsBMetric of $dsBMetric",
        description
      )
    else
      CheckResult(
        qcType,
        CheckStatus.Error,
        s"metric comparison failed. dsAMetric of $dsAMetric was compared to dsBMetric of $dsBMetric",
        description
      )
  }

  def applyCheckOnMetrics(
      dsAMetrics: Map[MetricDescriptor, MetricValue],
      dsBMetrics: Map[MetricDescriptor, MetricValue]
  )(implicit classTag: ClassTag[MV]): CheckResult = {
    val dsAMetricOpt: Option[MetricValue] = dsAMetrics.get(dsAMetricDescriptor)
    val dsBMetricOpt: Option[MetricValue] = dsBMetrics.get(dsBMetricDescriptor)
    (dsAMetricOpt, dsBMetricOpt) match {
      case (Some(dsAMetric), Some(dsBMetric)) =>
        (dsAMetric, dsBMetric) match {
          case (dsAMetric: MV, dsBMetric: MV) =>
            getCheckResult(metricComparator.fn(dsAMetric, dsBMetric), dsAMetric, dsBMetric)
          case _ => metricTypeErrorResult
        }
      case _ => metricNotPresentErrorResult
    }
  }
}
