package com.github.timgent.sparkdataquality.metrics

case class MetricComparator[MV <: MetricValue](description: String, fn: (MV, MV) => Boolean)

/**
  * Object for some helper functions to help with metric comparisons
  */
object MetricComparator {

  /**
    * Checks if 2 given metrics are equal
    * @tparam MV - The type of the metric
    * @return - a function which takes 2 metrics and returns true if they are equal, otherwise false
    */
  def metricsAreEqual[MV <: MetricValue]: MetricComparator[MV] =
    MetricComparator("metrics are equal", (dsMetricA, dsMetricB) => dsMetricA == dsMetricB)
}
