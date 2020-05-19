package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricCalculator.SizeMetricCalculator
/**
 * Describes the metric being calculated
 */
sealed trait MetricDescriptor {
  type MC <: MetricCalculator
  def metricCalculator: MC
  def toSimpleMetricDescriptor: SimpleMetricDescriptor
}

object MetricDescriptor {
  trait Filterable {
    def filter: MetricFilter
  }
  case class SizeMetricDescriptor(filter: MetricFilter = MetricFilter.noFilter) extends MetricDescriptor with Filterable {
    override def metricCalculator: SizeMetricCalculator = SizeMetricCalculator(filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor = SimpleMetricDescriptor("Size", Some(filter.filterDescription))
    override type MC = SizeMetricCalculator
  }
}

/**
 * Representation of a MetricDescriptor which is easy to persist
 */
private [sparkdataquality] case class SimpleMetricDescriptor(metricName: String,
                                                             filterDescription: Option[String])