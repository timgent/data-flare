package com.github.timgent.sparkdataquality.utils

import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

object DateTimeUtils {
  implicit class InstantExtension(instant: Instant) {
    def plusDays(n: Int, offset: ZoneOffset = ZoneOffset.UTC) =
      LocalDateTime.ofInstant(instant, offset).plusDays(n).toInstant(offset)
  }
}
