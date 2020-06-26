package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}

/**
  * Represents the value of a metric
  */
sealed trait MetricValue {
  type T
  def value: T
}

object MetricValue {
  sealed trait NumericMetricValue extends MetricValue

  case class LongMetric(value: Long) extends NumericMetricValue {
    type T = Long
  }
  case class DoubleMetric(value: Double) extends NumericMetricValue {
    type T = Double
  }
  implicit val constructLongMetric: Long => LongMetric = value => LongMetric(value)
}

sealed trait MetricValueConstructor[MV <: MetricValue] {
  def apply(value: MV#T): MV
}

object MetricValueConstructor {
  implicit val LongMetricConstructor = new MetricValueConstructor[LongMetric] {
    override def apply(value: Long): LongMetric = LongMetric(value)
  }

  implicit val DoubleMetricConstructor = new MetricValueConstructor[DoubleMetric] {
    override def apply(value: Double): DoubleMetric = DoubleMetric(value)
  }
}