package utils

import qualitychecker.DeequCheckStatus
import qualitychecker.checks.CheckStatus

object DeequUtils {
  implicit class DeequCheckStatusEnricher(checkStatus: DeequCheckStatus) {
    def toCheckStatus = checkStatus match {
      case DeequCheckStatus.Success => CheckStatus.Success
      case DeequCheckStatus.Warning => CheckStatus.Warning
      case DeequCheckStatus.Error => CheckStatus.Error
    }
  }
}
