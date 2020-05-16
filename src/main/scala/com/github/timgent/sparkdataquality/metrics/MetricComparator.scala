package com.github.timgent.sparkdataquality.metrics

object MetricComparator {
  def metricsAreEqual[MV <: MetricValue]: (MV, MV) => Boolean =
    (dsMetricA, dsMetricB) => dsMetricA == dsMetricB
}