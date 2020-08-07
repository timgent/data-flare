package com.github.timgent.dataflare.metrics

import com.github.timgent.dataflare.FlareError.MetricCalculationError
import com.github.timgent.dataflare.checkssuite.DescribedDs
import com.github.timgent.dataflare.utils.CommonFixtures.NumberString
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
      ).toDS
      val dds = DescribedDs(ds, "ds")

      val sizeMetricDescriptor = MetricDescriptor.SizeMetric(MetricFilter.noFilter)

      val calculatedMetrics: Either[MetricCalculationError, Map[MetricDescriptor, MetricValue]] =
        MetricsCalculator.calculateMetrics(dds, Seq(sizeMetricDescriptor))

      calculatedMetrics shouldBe Right(
        Map(
          sizeMetricDescriptor -> MetricValue.LongMetric(2)
        )
      )
    }

    "be able to calculate multiple metrics" in {
      pending
    }

    "return an error if an invalid MetricDescriptor is given" in {
      val ds = Seq(
        NumberString(1, "a"),
        NumberString(2, "b")
      ).toDS
      val dds = DescribedDs(ds, "ds")

      val badDescriptor = MetricDescriptor.CountDistinctValuesMetric(onColumns = List("nonexistent_column"))

      val Left(metricsCalculationErr) = MetricsCalculator.calculateMetrics(dds, Seq(badDescriptor))

      metricsCalculationErr.dds shouldBe dds
      metricsCalculationErr.metricDescriptors shouldBe Seq(badDescriptor)
    }
  }
}
