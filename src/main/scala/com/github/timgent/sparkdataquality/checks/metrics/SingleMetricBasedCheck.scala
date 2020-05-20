package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.{ComplianceMetricDescriptor, DistinctValuesMetricDescriptor, SizeMetricDescriptor}
import com.github.timgent.sparkdataquality.metrics.{ComplianceFn, MetricDescriptor, MetricFilter, MetricValue}
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

  sealed trait ThresholdBasedCheck[MV <: MetricValue] extends SingleMetricBasedCheck[MV] {
    def checkShortName: String

    def threshold: AbsoluteThreshold[MV#T]

    def applyCheck(metric: MV): CheckResult = {
      if (threshold.isWithinThreshold(metric.value)) {
        CheckResult(CheckStatus.Success, s"$checkShortName of ${metric.value} was within the range $threshold", description)
      } else {
        CheckResult(CheckStatus.Error, s"$checkShortName of ${metric.value} was outside the range $threshold", description)
      }
    }
  }

  case class SizeCheck(threshold: AbsoluteThreshold[Long], filter: MetricFilter) extends ThresholdBasedCheck[MetricValue.LongMetric] {
    override def checkShortName: String = "Size"

    override def description: String = s"SizeCheck with filter: ${filter.filterDescription}"

    override def metricDescriptor: MetricDescriptor = SizeMetricDescriptor(filter)
  }

  case class ComplianceCheck(threshold: AbsoluteThreshold[Double], complianceFn: ComplianceFn,
                             filter: MetricFilter = MetricFilter.noFilter) extends ThresholdBasedCheck[MetricValue.DoubleMetric] {

    override def checkShortName: String = "Compliance"

    override def description: String = s"ComplianceCheck with filter: ${filter.filterDescription}"

    override def metricDescriptor: MetricDescriptor = ComplianceMetricDescriptor(complianceFn, filter)
  }

  case class DistinctValuesCheck(threshold: AbsoluteThreshold[Long],
                                 onColumns: List[String],
                                 filter: MetricFilter = MetricFilter.noFilter) extends ThresholdBasedCheck[MetricValue.LongMetric] {

    override def checkShortName: String = "DistinctValues"

    override def description: String = s"DistinctValuesCheck with filter: ${filter.filterDescription}"

    override def metricDescriptor: MetricDescriptor = DistinctValuesMetricDescriptor(onColumns, filter)
  }

}