package com.github.timgent.sparkdataquality.checks

import enumeratum._

/**
 * Represents a check to be done
 */
trait QCCheck {
  def description: String

  def qcType: QcType
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

private [sparkdataquality] sealed trait QcType extends EnumEntry

private [sparkdataquality] object QcType extends Enum[QcType] {
  val values = findValues
  case object DeequQualityCheck extends QcType
  case object SingleDatasetQualityCheck extends QcType
  case object DatasetComparisonQualityCheck extends QcType
  case object ArbitraryQualityCheck extends QcType
  case object MetricsBasedQualityCheck extends QcType
  case object GenericChecks extends QcType
}
