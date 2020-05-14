package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricValue.LongMetric
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row}

sealed trait MetricCalculator {
  type MetricType <: MetricValue
  def wrapMetricValue(metricValue: MetricType#T): MetricType
}

object MetricCalculator {
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
}
