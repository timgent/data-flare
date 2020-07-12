package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricValue.NumericMetricValue
import spire.implicits._
import spire.math.Numeric

/**
  * Comparison to apply between 2 metrics
  *
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

  def withinXPercent[MV <: NumericMetricValue](percentage: Double)(implicit numeric: Numeric[MV#T]) =
    MetricComparator[MV](
      s"within${(percentage).toString}PercentComparator",
      (metric1, metric2) => {
        val perc = (100 + percentage) / 100
        val perc2 = (100 - percentage) / 100
        val m1D = metric1.toDouble
        val m2D = metric2.toDouble
        m2D <= (m1D * perc) && m2D >= (m1D * perc2)
      }
    )
}
