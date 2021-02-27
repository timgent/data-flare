package com.github.timgent.dataflare.metrics

import com.github.timgent.dataflare.metrics.MetricValue.{DoubleMetric, LongMetric, OptDoubleMetric, OptLongMetric}

/**
  * Represents the value of a metric
  */
sealed trait MetricValue {
  type T
  def value: T
}

object MetricValue {
  sealed trait NumericMetricValue extends MetricValue
  sealed trait OptNumericMetricValue extends MetricValue {
    type U
    type T = Option[U]
  }

  case class LongMetric(value: Long) extends NumericMetricValue {
    type T = Long
  }
  case class DoubleMetric(value: Double) extends NumericMetricValue {
    type T = Double
  }
  case class OptLongMetric(value: Option[Long]) extends OptNumericMetricValue {
    type U = Long
  }

  case class OptDoubleMetric(value: Option[Double]) extends OptNumericMetricValue {
    type U = Double
  }

  implicit val constructLongMetric: Long => LongMetric = value => LongMetric(value)
}

sealed trait MetricValueConstructor[MV <: MetricValue] {
  def apply(value: MV#T): MV
  def zero: MV#T
  def maxValue: MV#T
  def minValue: MV#T
}

object MetricValueConstructor {
  implicit val LongMetricConstructor = new MetricValueConstructor[LongMetric] {
    override def zero: Long = 0L
    override def minValue: Long = Long.MinValue
    override def maxValue: Long = Long.MaxValue
    override def apply(value: Long): LongMetric = LongMetric(value)
  }
  implicit val OptLongMetricConstructor = new MetricValueConstructor[OptLongMetric] {
    override def apply(value: Option[Long]): OptLongMetric = OptLongMetric(value)
    override def zero: Option[Long] = None
    override def maxValue: Option[Long] = Some(Long.MaxValue)
    override def minValue: Option[Long] = Some(Long.MinValue)
  }
  implicit val OptDoubleMetricConstructor = new MetricValueConstructor[OptDoubleMetric] {
    override def apply(value: Option[Double]): OptDoubleMetric = OptDoubleMetric(value)
    override def zero: Option[Double] = None
    override def maxValue: Option[Double] = Some(Double.MaxValue)
    override def minValue: Option[Double] = Some(Double.MinValue)
  }

  implicit val DoubleMetricConstructor = new MetricValueConstructor[DoubleMetric] {
    override def zero: Double = 0.0
    override def minValue: Double = Double.MinValue
    override def maxValue: Double = Double.MaxValue
    override def apply(value: Double): DoubleMetric = DoubleMetric(value)
  }
}
