package com.github.timgent.sparkdataquality.metrics

/**
  * Comparison to apply between 2 metrics
  * @param description - description of what this comparison does
  * @param fn - the function which compares 2 metrics and returns true is the comparison passes, otherwise returns false
  * @tparam MV - the type of the metric values being compared
  */
case class MetricComparator[MV <: MetricValue](description: String, fn: (MV#T, MV#T) => Boolean)

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
