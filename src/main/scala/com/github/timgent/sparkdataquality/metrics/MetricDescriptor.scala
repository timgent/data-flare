package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricCalculator.SizeMetricCalculator
/**
 * Describes the metric being calculated
 */
sealed trait MetricDescriptor {
  type MC <: MetricCalculator
  def metricCalculator: MC
}

object MetricDescriptor {
  trait Filterable {
    def filter: MetricFilter
  }
  case class SizeMetricDescriptor(filter: MetricFilter = MetricFilter.noFilter) extends MetricDescriptor with Filterable {
    override def metricCalculator: SizeMetricCalculator = SizeMetricCalculator(filter)
    override type MC = SizeMetricCalculator
  }
}
