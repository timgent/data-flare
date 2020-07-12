package com.github.timgent.sparkdataquality

import com.github.timgent.sparkdataquality.checkssuite.DescribedDs
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor

sealed trait SdqError

object SdqError {
  case class MetricCalculationError(dds: DescribedDs, metricDescriptors: Seq[MetricDescriptor], err: Throwable)
}
