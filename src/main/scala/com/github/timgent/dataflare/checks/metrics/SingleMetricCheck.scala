package com.github.timgent.dataflare.checks.metrics

import com.github.timgent.dataflare.checks.CheckDescription.SingleMetricCheckDescription
import com.github.timgent.dataflare.checks.QCCheck.SingleDsCheck
import com.github.timgent.dataflare.checks.{CheckDescription, CheckResult, CheckStatus, QcType, RawCheckResult}
import com.github.timgent.dataflare.metrics.MetricDescriptor.{
  ComplianceMetric,
  CountDistinctValuesMetric,
  DistinctnessMetric,
  MaxValueMetric,
  MinValueMetric,
  SizeMetric,
  SumValuesMetric
}
import com.github.timgent.dataflare.metrics.MetricValue.{DoubleMetric, LongMetric, NumericMetricValue, OptNumericMetricValue}
import com.github.timgent.dataflare.metrics.{ComplianceFn, MetricDescriptor, MetricFilter, MetricValue, MetricValueConstructor}
import com.github.timgent.dataflare.thresholds.AbsoluteThreshold

import scala.reflect.ClassTag

/**
  * A check based on a single metric
  * @param metric - describes the metric the check will be done on
  * @param checkDescription - the user friendly description for this check
  * @param check - the check to be done
  * @tparam MV - the type of the MetricValue that will be calculated in order to complete this check
  */
case class SingleMetricCheck[MV <: MetricValue](metric: MetricDescriptor { type MetricType = MV }, checkDescription: String)(
    check: MV#T => RawCheckResult
) extends MetricsBasedCheck
    with SingleDsCheck {

  override def description: CheckDescription = SingleMetricCheckDescription(checkDescription, metric.toSimpleMetricDescriptor)

  def applyCheck(metric: MV): CheckResult = {
    check(metric.value).withDescription(QcType.SingleMetricCheck, description)
  }

  // typeTag required here to enable match of metric on type MV. Without class tag this type check would be fruitless
  private[dataflare] final def applyCheckOnMetrics(
      metrics: Map[MetricDescriptor, MetricValue]
  )(implicit classTag: ClassTag[MV]): CheckResult = {
    val metricOfInterestOpt: Option[MetricValue] =
      metrics.get(metric).map(metricValue => metricValue)
    metricOfInterestOpt match {
      case Some(metric) =>
        metric match { // TODO: Look into heterogenous maps to avoid this type test - https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-1.2.4#heterogenous-maps
          case metric: MV => applyCheck(metric)
          case _          => metricTypeErrorResult
        }
      case None => metricNotPresentErrorResult
    }
  }
}

object SingleMetricCheck {

  /**
    * A check based on a single metric that checks if that metric is within the given threshold
    * @param metricDescriptor - describes the metric the check will be done on
    * @param description - the user friendly description for this check
    * @param threshold - the threshold that the metric must be within to pass
    * @tparam MV - the type of the MetricValue that will be calculated in order to complete this check
    * @return
    */
  def thresholdBasedCheck[MV <: MetricValue](
      metricDescriptor: MetricDescriptor { type MetricType = MV },
      description: String,
      threshold: AbsoluteThreshold[MV#T]
  ): SingleMetricCheck[MV] = {
    SingleMetricCheck(metricDescriptor, description) { metricValue: MV#T =>
      if (threshold.isWithinThreshold(metricValue)) {
        RawCheckResult(
          CheckStatus.Success,
          s"${metricDescriptor.metricName} of ${metricValue} was within the range $threshold"
        )
      } else {
        RawCheckResult(
          CheckStatus.Error,
          s"${metricDescriptor.metricName} of ${metricValue} was outside the range $threshold"
        )
      }
    }
  }

  /**
    * A check based on a single optional metric that checks if that metric is within the given threshold. If the metric
    * has a value of None the check will automatically fail
    * @param metricDescriptor - describes the metric the check will be done on
    * @param description - the user friendly description for this check
    * @param threshold - the threshold that the metric must be within to pass
    * @tparam MV - the type of the MetricValue that will be calculated in order to complete this check
    * @return
    */
  def optThresholdBasedCheck[MV <: OptNumericMetricValue](
      metricDescriptor: MetricDescriptor { type MetricType = MV },
      description: String,
      threshold: AbsoluteThreshold[MV#U]
  ): SingleMetricCheck[MV] = {
    SingleMetricCheck(metricDescriptor, description) { maybeMetricValue: MV#T =>
      maybeMetricValue match {
        case Some(metricValue) if threshold.isWithinThreshold(metricValue) =>
          RawCheckResult(
            CheckStatus.Success,
            s"${metricDescriptor.metricName} of ${metricValue} was within the range $threshold"
          )
        case Some(metricValue) =>
          RawCheckResult(
            CheckStatus.Error,
            s"${metricDescriptor.metricName} of ${metricValue} was outside the range $threshold"
          )
        case None =>
          RawCheckResult(
            CheckStatus.Success,
            s"${metricDescriptor.metricName} returned a value of None which was not within the range $threshold"
          )
      }
    }
  }

  /**
    * Checks the count of rows in a dataset after the given filter is applied is within the given threshold
    * @param threshold
    * @param filter - filter to be applied before rows are counted
    */
  def sizeCheck(threshold: AbsoluteThreshold[Long], filter: MetricFilter = MetricFilter.noFilter): SingleMetricCheck[LongMetric] =
    thresholdBasedCheck[LongMetric](SizeMetric(filter), s"SizeCheck", threshold)

  /**
    * Checks the sum of value of rows in a dataset for a given col after the given filter is applied
    * is within the given threshold
    * @param threshold
    * @param onColumn
    * @param filter
    * @tparam MV
    * @return
    */
  def sumValueCheck[MV <: NumericMetricValue: MetricValueConstructor](
      threshold: AbsoluteThreshold[MV#T],
      onColumn: String,
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricCheck[MV] =
    thresholdBasedCheck[MV](SumValuesMetric(onColumn, filter), "MinValueCheck", threshold)

  /**
    * Checks the min value of rows in a dataset for a given col after the given filter is applied
    * is within the given threshold
    * @param threshold the threshold for what fraction of rows is acceptable
    * @param onColumn column on which min value needs to be computed
    * @param filter the filter that is applied before the dataset min value is computed
    * @return
    */
  def minValueCheck[MV <: OptNumericMetricValue: MetricValueConstructor](
      threshold: AbsoluteThreshold[MV#U],
      onColumn: String,
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricCheck[MV] =
    optThresholdBasedCheck[MV](MinValueMetric[MV](onColumn, filter), "MinValueCheck", threshold)

  /**
    * Checks the max value of rows in a dataset for a given col after the given filter is applied
    * is within the given threshold
    * @param threshold the threshold for what fraction of rows is acceptable
    * @param onColumn column on which max value needs to be computed
    * @param filter the filter that is applied before the dataset max value is computed
    * @tparam MV
    * @return
    */
  def maxValueCheck[MV <: OptNumericMetricValue: MetricValueConstructor](
      threshold: AbsoluteThreshold[MV#T],
      onColumn: String,
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricCheck[MV] =
    thresholdBasedCheck[MV](MaxValueMetric(onColumn, filter), "MaxValueCheck", threshold)

  /**
    * Checks the fraction of rows that are compliant with the given complianceFn
    * @param threshold - the threshold for what fraction of rows is acceptable
    * @param complianceFn - the function rows are tested with to see if they are compliant
    * @param filter - the filter that is applied before the compliance fraction is calculated
    */
  def complianceCheck(
      threshold: AbsoluteThreshold[Double],
      complianceFn: ComplianceFn,
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricCheck[DoubleMetric] =
    thresholdBasedCheck[DoubleMetric](
      ComplianceMetric(complianceFn, filter),
      s"ComplianceCheck",
      threshold
    )

  /**
    * Checks the number of distinct values across the given columns
    * @param threshold - the threshold for what number of distinct values is acceptable
    * @param onColumns - the columns to check for distinct values in
    * @param filter - the filter that is applied before the distinct value count is done
    */
  def distinctValuesCheck(
      threshold: AbsoluteThreshold[Long],
      onColumns: List[String],
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricCheck[LongMetric] =
    thresholdBasedCheck[LongMetric](
      CountDistinctValuesMetric(onColumns, filter),
      s"DistinctValuesCheck on columns: $onColumns with filter: ${filter.filterDescription}",
      threshold
    )

  /**
    * Checks the distinctness level of values across the given columns (result of 1 means every value is distinct)
    * @param threshold - the threshold for what number of distinct values is acceptable
    * @param onColumns - the columns to check for distinct values in
    * @param filter - the filter that is applied before the distinct value count is done
    */
  def distinctnessCheck(
      threshold: AbsoluteThreshold[Double],
      onColumns: List[String],
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricCheck[DoubleMetric] =
    thresholdBasedCheck[DoubleMetric](
      DistinctnessMetric(onColumns, filter),
      s"DistinctnessCheck on columns: $onColumns with filter: ${filter.filterDescription}",
      threshold
    )
}
