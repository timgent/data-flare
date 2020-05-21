package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row}

private[sparkdataquality] sealed trait MetricCalculator {
  type MetricType <: MetricValue

  def wrapMetricValue(metricValue: MetricType#T): MetricType
}

private[sparkdataquality] object MetricCalculator {

  /**
   * MetricCalculator that calculates metrics based on a simple aggregation function on the whole dataset
   */
  sealed trait SimpleMetricCalculator extends MetricCalculator {
    def aggFunction: Column

    def valueFromRow(row: Row, index: Int): MetricType = wrapMetricValue(row.getAs[MetricType#T](index))

    def filter: MetricFilter
  }

  case class SizeMetricCalculator(filter: MetricFilter) extends SimpleMetricCalculator {
    override type MetricType = LongMetric

    override def aggFunction: Column = {
      sum(when(filter.filter, 1).otherwise(0))
    }

    override def wrapMetricValue(metricValue: Long): LongMetric = LongMetric(metricValue)
  }

  case class ComplianceMetricCalculator(complianceFn: ComplianceFn, filter: MetricFilter) extends SimpleMetricCalculator {
    override type MetricType = DoubleMetric

    override def aggFunction: Column = {
      sum(when(filter.filter and complianceFn.definition, 1).otherwise(0)) /
        sum(when(filter.filter, 1).otherwise(0))
    }

    override def wrapMetricValue(metricValue: Double): DoubleMetric = DoubleMetric(metricValue)
  }

  case class DistinctValuesMetricCalculator(onColumns: List[String], filter: MetricFilter) extends SimpleMetricCalculator {
    override type MetricType = LongMetric

    override def aggFunction: Column = {
      val countDistinctCols: List[Column] = onColumns.map(onColumn => when(not(filter.filter), null).otherwise(col(onColumn)))
      countDistinct(countDistinctCols.head, countDistinctCols.tail: _*) // TODO: Handle empty col list case and bad filter case
    }

    override def wrapMetricValue(metricValue: Long): LongMetric = LongMetric(metricValue)
  }

}
