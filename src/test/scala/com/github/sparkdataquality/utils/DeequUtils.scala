package com.github.sparkdataquality.utils

import com.github.sparkdataquality.checks.CheckStatus
import com.github.sparkdataquality.sparkdataquality.DeequCheckStatus

object DeequUtils {
  implicit class DeequCheckStatusEnricher(checkStatus: DeequCheckStatus) {
    def toCheckStatus = checkStatus match {
      case DeequCheckStatus.Success => CheckStatus.Success
      case DeequCheckStatus.Warning => CheckStatus.Warning
      case DeequCheckStatus.Error => CheckStatus.Error
    }
  }
}
