package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.CheckDescription.SimpleCheckDescription

/**
  * Arbitrary check - could provide any function to do this type of check
  */
trait ArbitraryCheck extends QCCheck {
  def description: CheckDescription

  override def qcType: QcType = QcType.ArbitraryCheck

  def applyCheck: CheckResult
}

object ArbitraryCheck {
  def apply(checkDescription: String)(check: => RawCheckResult): ArbitraryCheck =
    new ArbitraryCheck {
      override def description: CheckDescription = SimpleCheckDescription(checkDescription)

      override def applyCheck: CheckResult = check.withDescription(qcType, description, None)
    }
}
