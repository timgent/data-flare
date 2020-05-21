package com.github.timgent.sparkdataquality.checks

import enumeratum._

/**
 * Represents a check to be done
 */
trait QCCheck {
  def description: String
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