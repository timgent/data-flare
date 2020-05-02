package com.github.timgent.sparkdataquality.utils

import com.github.timgent.sparkdataquality.checks.CheckStatus.{Error, Success, Warning}
import com.github.timgent.sparkdataquality.sparkdataquality.DeequCheckStatus

object DeequUtils {

  implicit class DeequCheckStatusEnricher(checkStatus: DeequCheckStatus) {
    def toCheckStatus = checkStatus match {
      case DeequCheckStatus.Success => Success
      case DeequCheckStatus.Warning => Warning
      case DeequCheckStatus.Error => Error
    }
  }

}
