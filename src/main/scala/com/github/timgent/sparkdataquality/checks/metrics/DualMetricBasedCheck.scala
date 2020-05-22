package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import com.github.timgent.sparkdataquality.metrics.{MetricDescriptor, MetricValue}

import scala.reflect.ClassTag

/**
 * A check based on a metric for one dataset compared to a metric on another dataset
 *
 * @param dsAMetricDescriptor - the metric to be used on dataset a
 * @param dsBMetricDescriptor - the metric to be used on dataset b
 * @param metricComparator    - comparison function for the metrics which determines if the check passes
 * @param description         - description of the check
 * @tparam MV
 */
final case class DualMetricBasedCheck[MV <: MetricValue](dsAMetricDescriptor: MetricDescriptor,
                                                         dsBMetricDescriptor: MetricDescriptor,
                                                         description: String)(
                                                         metricComparator: (MV, MV) => Boolean) extends MetricsBasedCheck {

  private def getCheckResult(checkPassed: Boolean): CheckResult = {
    if (checkPassed)
      CheckResult(CheckStatus.Success, "metric comparison passed", description)
    else
      CheckResult(CheckStatus.Error, "metric comparison failed", description)
  }

  def applyCheckOnMetrics(dsAMetrics: Map[MetricDescriptor, MetricValue],
                          dsBMetrics: Map[MetricDescriptor, MetricValue])(implicit classTag: ClassTag[MV]): CheckResult = {
    val dsAMetricOpt: Option[MetricValue] = dsAMetrics.get(dsAMetricDescriptor)
    val dsBMetricOpt: Option[MetricValue] = dsBMetrics.get(dsBMetricDescriptor)
    (dsAMetricOpt, dsBMetricOpt) match {
      case (Some(dsAMetric), Some(dsBMetric)) => (dsAMetric, dsBMetric) match {
        case (dsAMetric: MV, dsBMetric: MV) => getCheckResult(metricComparator(dsAMetric, dsBMetric))
        case _ => metricTypeErrorResult
      }
      case _ => metricNotPresentErrorResult
    }
  }
}
