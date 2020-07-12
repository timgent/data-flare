package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.metrics.MetricComparator // DO NOT DELETE - required for test
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SumValuesMetric // DO NOT DELETE - required for test
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric} // DO NOT DELETE - required for test
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DualMetricCheckTest extends AnyWordSpec with Matchers {
  "DualMetricCheck" should {
    "not compile" when {
      "the metric type doesn't match the comparator type" in {
        """
          |DualMetricCheck(
          |          SumValuesMetric[DoubleMetric](""),
          |          SumValuesMetric[DoubleMetric](""),
          |          "change in unprojected patients",
          |          MetricComparator[LongMetric](???, ???)
          |        )
          |""".stripMargin shouldNot compile
      }
    }
    "compile" when {
      "the metric type matches the comparator type" in {
        """
          |DualMetricCheck(
          |          SumValuesMetric[LongMetric](""),
          |          SumValuesMetric[LongMetric](""),
          |          "change in unprojected patients",
          |          MetricComparator[LongMetric](???, ???)
          |        )
          |""".stripMargin should compile
      }
    }
  }
}
