package com.github.timgent.sparkdataquality.metrics

/**
  * Represents the value of a metric
  */
sealed trait MetricValue {
  type T
  def value: T
}

object MetricValue {
  case class LongMetric(value: Long) extends MetricValue {
    type T = Long
  }
  case class DoubleMetric(value: Double) extends MetricValue {
    type T = Double
  }
  implicit val constructLongMetric: Long => LongMetric = value => LongMetric(value)
}
