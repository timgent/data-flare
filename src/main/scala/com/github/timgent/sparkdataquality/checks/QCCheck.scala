package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.metrics.SimpleMetricDescriptor
import enumeratum._

sealed trait CheckDescription

object CheckDescription {
  case class SimpleCheckDescription(desc: String) extends CheckDescription
  case class DualMetricCheckDescription(
      desc: String,
      dsMetric: SimpleMetricDescriptor,
      dsToCompareMetric: SimpleMetricDescriptor,
      metricComparator: String
  ) extends CheckDescription
  case class SingleMetricCheckDescription(
      desc: String,
      dsMetric: SimpleMetricDescriptor
  ) extends CheckDescription
}

/**
  * Represents a check to be done
  */
trait QCCheck {
  def description: CheckDescription

  def qcType: QcType
}

object QCCheck {
  trait SingleDsCheck extends QCCheck
  trait DualDsQCCheck extends QCCheck
}

/**
  * Represents the resulting status of a check
  */
sealed trait CheckStatus extends EnumEntry

object CheckStatus extends Enum[CheckStatus] {
  val values = findValues

  case object Success extends CheckStatus

  case object Warning extends CheckStatus

  case object Error extends CheckStatus
}

private[sparkdataquality] sealed trait QcType extends EnumEntry

private[sparkdataquality] object QcType extends Enum[QcType] {
  val values = findValues
  case object DeequQualityCheck extends QcType
  case object ArbSingleDsCheck extends QcType
  case object ArbDualDsCheck extends QcType
  case object ArbitraryCheck extends QcType
  case object SingleMetricCheck extends QcType
  case object DualMetricCheck extends QcType
}
