package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SizeMetricDescriptor
import com.github.timgent.sparkdataquality.metrics.{MetricDescriptor, MetricFilter, MetricValue}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold

import scala.reflect.ClassTag

trait SingleMetricBasedCheck[MV <: MetricValue] extends MetricsBasedCheck {
  def metricDescriptor: MetricDescriptor

  protected def applyCheck(metric: MV): CheckResult

  // typeTag required here to enable match of metric on type MV. Without class tag this type check would be fruitless
  final def applyCheckOnMetrics(metrics: Map[MetricDescriptor, MetricValue])(implicit classTag: ClassTag[MV]): CheckResult = {
    val metricOfInterestOpt: Option[MetricValue] =
      metrics.get(metricDescriptor).map(metricValue => metricValue)
    metricOfInterestOpt match {
      case Some(metric) =>
        metric match { // TODO: Look into heterogenous maps to avoid this type test - https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-1.2.4#heterogenous-maps
          case metric: MV => applyCheck(metric)
          case _ => metricTypeErrorResult
        }
      case None => metricNotPresentErrorResult
    }
  }
}

object SingleMetricBasedCheck {

  case class SizeCheck(threshold: AbsoluteThreshold[Long], filter: MetricFilter) extends SingleMetricBasedCheck[MetricValue.LongMetric] {
    override protected def applyCheck(metric: MetricValue.LongMetric): CheckResult = {
      val sizeIsWithinThreshold = threshold.isWithinThreshold(metric.value)
      if (sizeIsWithinThreshold) {
        CheckResult(CheckStatus.Success, s"Size of ${metric.value} was within the range $threshold", description)
      } else {
        CheckResult(CheckStatus.Error, s"Size of ${metric.value} was outside the range $threshold", description)
      }
    }

    override def description: String = s"SizeCheck with filter: ${filter.filterDescription}"

    override def metricDescriptor: MetricDescriptor = SizeMetricDescriptor(filter)
  }

}