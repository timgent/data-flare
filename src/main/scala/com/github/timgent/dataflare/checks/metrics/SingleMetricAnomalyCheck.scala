package com.github.timgent.dataflare.checks.metrics

import java.time.Instant

import com.github.timgent.dataflare.FlareError.{LookedUpMetricOfWrongType, MetricMissing}
import com.github.timgent.dataflare.checks.CheckDescription.SingleMetricCheckDescription
import com.github.timgent.dataflare.checks.QCCheck.SingleDsCheck
import com.github.timgent.dataflare.checks._
import com.github.timgent.dataflare.metrics.MetricValue.LongMetric
import com.github.timgent.dataflare.metrics.{MetricDescriptor, MetricValue}

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

  override def description: CheckDescription =
    SingleMetricCheckDescription(checkDescription, metric.toSimpleMetricDescriptor) // TODO: Should have more info in the description

  def applyCheck(metric: MV, historicMetrics: Map[Instant, MV#T]): CheckResult = {
    check(metric.value, historicMetrics).withDescription(QcType.SingleMetricAnomalyCheck, description)
  }

  // typeTag required here to enable match of metric on type MV. Without class tag this type check would be fruitless
  private[dataflare] final def applyCheckOnMetrics(
      metrics: Map[MetricDescriptor, MetricValue],
      historicMetrics: Map[Instant, MetricValue]
  )(implicit classTag: ClassTag[MV]): CheckResult = {
    val maybeMetricOfInterest = getMetric(metric, metrics)
    val relevantHistoricMetrics: Map[Instant, MV#T] = historicMetrics.collect {
      case (instant, metricValue: MV) => (instant, metricValue.value)
    }
    maybeMetricOfInterest match {
      case Left(MetricMissing)             => metricNotPresentErrorResult
      case Left(LookedUpMetricOfWrongType) => metricTypeErrorResult
      case Right(metric)                   => applyCheck(metric, relevantHistoricMetrics)
    }
  }

}

object SingleMetricAnomalyCheck {
  def absoluteChangeAnomalyCheck(
      maxReduction: Long,
      maxIncrease: Long,
      metricDescriptor: MetricDescriptor.Aux[LongMetric] // SizeMetric
  ): SingleMetricAnomalyCheck[LongMetric] =
    SingleMetricAnomalyCheck[LongMetric](metricDescriptor, "AbsoluteChangeAnomalyCheck") { (currentMetricValue, historicMetricValues) =>
      val (_, lastMetricValue) = historicMetricValues.maxBy(_._1)
      val isWithinAcceptableRange =
        (lastMetricValue + maxIncrease) <= currentMetricValue && (lastMetricValue - maxReduction) >= currentMetricValue
      if (isWithinAcceptableRange)
        RawCheckResult(
          CheckStatus.Success,
          s"MetricValue of $currentMetricValue was not anomalous compared to previous result of $lastMetricValue"
        )
      else
        RawCheckResult(
          CheckStatus.Error,
          s"MetricValue of $currentMetricValue was anomalous compared to previous result of $lastMetricValue"
        )
    }

  def stdChangeAnomalyCheck(
      lowerDeviationFactor: Int = 2,
      higherDeviationFactor: Int = 2,
      metricDescriptor: MetricDescriptor.Aux[LongMetric]
  ): SingleMetricAnomalyCheck[LongMetric] = {

    SingleMetricAnomalyCheck[LongMetric](metricDescriptor, "STDChangeAnomalyCheck") {
      (currentMetricValue: Long, historicMetricValues: Map[Instant, Long]) =>
        import com.github.timgent.dataflare.utils.stats.{mean, stdDev}

        val historicValues = historicMetricValues.values

        val historicMean: Double = mean(historicValues)
        val historicSTD: Double = stdDev(historicValues)

        val isValidRange =
          currentMetricValue >= historicMean - (lowerDeviationFactor * historicSTD) &&
            currentMetricValue <= (higherDeviationFactor * historicSTD) + historicMean

        if (isValidRange)
          RawCheckResult(
            CheckStatus.Success,
            s"MetricValue of $currentMetricValue was not anomalous compared to previous results. Mean: $historicMean; STD: $historicSTD"
          )
        else
          RawCheckResult(
            CheckStatus.Error,
            s"MetricValue of $currentMetricValue was anomalous compared to previous results. Mean: $historicMean; STD: $historicSTD"
          )

    }
  }

}
