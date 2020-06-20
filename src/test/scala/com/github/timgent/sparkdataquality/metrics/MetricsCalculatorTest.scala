package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.utils.CommonFixtures.NumberString
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsCalculatorTest extends AnyWordSpec with DatasetSuiteBase with Matchers {
  import spark.implicits._
  "MetricCalculator" should {
    "be able to calculate a simple metric" in {
      val ds = Seq(
        NumberString(1, "a"),
        NumberString(2, "b")
      )

      val sizeMetricDescriptor = MetricDescriptor.SizeMetricDescriptor(MetricFilter.noFilter)

      val calculatedMetrics: Map[MetricDescriptor, MetricValue] =
        MetricsCalculator.calculateMetrics(ds.toDS, Seq(sizeMetricDescriptor))

      calculatedMetrics shouldBe Map(
        sizeMetricDescriptor -> MetricValue.LongMetric(2)
      )
    }

    "be able to calculate multiple metrics" in {
      pending
    }
  }
}
