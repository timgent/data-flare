package com.github.timgent.dataflare.checks.metrics

import java.time.Instant

import com.github.timgent.dataflare.checks.CheckDescription.SingleMetricCheckDescription
import com.github.timgent.dataflare.checks.QCCheck.SingleDsCheck
import com.github.timgent.dataflare.checks.{CheckDescription, CheckResult, QcType, RawCheckResult}
import com.github.timgent.dataflare.metrics.MetricValue.LongMetric
import com.github.timgent.dataflare.metrics.{MetricDescriptor, MetricValue}
import com.github.timgent.dataflare.repository.MetricsPersister

import scala.reflect.ClassTag

/**
  * A check based on a single metric and the history of that metric, in order to detect anomalies
  *
  * @param metric - describes the metric the check will be done on
  * @param checkDescription - the user friendly description for this check
  * @param check - the check to be done
  * @tparam MV - the type of the MetricValue that will be calculated in order to complete this check
  */
case class SingleMetricAnomalyCheck[MV <: MetricValue](metric: MetricDescriptor { type MetricType = MV }, checkDescription: String)(
    check: (MV#T, Map[Instant, MV#T]) => RawCheckResult
) extends MetricsBasedCheck
    with SingleDsCheck {

  override def qcType: QcType = QcType.SingleMetricAnomalyCheck

  override def description: CheckDescription = SingleMetricCheckDescription(checkDescription, metric.toSimpleMetricDescriptor)

  def applyCheck(metric: MV, historicMetrics: Map[Instant, MV#T]): CheckResult = {
    check(metric.value, historicMetrics).withDescription(QcType.SingleMetricCheck, description)
  }
}

object SingleMetricAnomalyCheck {
  def absoluteChangeAnomalyCheck(
      maxReduction: Long,
      maxIncrease: Long,
      metricDescriptor: MetricDescriptor
  ): SingleMetricAnomalyCheck[LongMetric] = ???
}
