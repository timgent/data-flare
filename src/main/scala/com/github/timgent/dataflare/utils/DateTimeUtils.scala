package com.github.timgent.dataflare.utils

import java.time.{Instant, LocalDateTime, ZoneOffset}

object DateTimeUtils {
  implicit class InstantExtension(instant: Instant) {
    def plusDays(n: Int, offset: ZoneOffset = ZoneOffset.UTC) =
      LocalDateTime.ofInstant(instant, offset).plusDays(n).toInstant(offset)
  }
}
