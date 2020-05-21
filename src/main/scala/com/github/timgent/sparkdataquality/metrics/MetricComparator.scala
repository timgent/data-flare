package com.github.timgent.sparkdataquality.metrics

/**
 * Object for some helper functions to help with metric comparisons
 */
object MetricComparator {
  /**
   * Checks if 2 given metrics are equal
   * @tparam MV - The type of the metric
   * @return - a function which takes 2 metrics and returns true if they are equal, otherwise false
   */
  def metricsAreEqual[MV <: MetricValue]: (MV, MV) => Boolean =
    (dsMetricA, dsMetricB) => dsMetricA == dsMetricB
}