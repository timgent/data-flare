package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricComparatorTest extends AnyWordSpec with Matchers {
  "MetricComparator.withinXPercentComparator" should {
    "pass if metrics are within X percent of each other" in {
      MetricComparator.withinXPercent[LongMetric](10).fn(10, 11) shouldBe true
      MetricComparator.withinXPercent[DoubleMetric](10).fn(10d, 11d) shouldBe true
      MetricComparator.withinXPercent[LongMetric](10).fn(10, 9) shouldBe true
      MetricComparator.withinXPercent[DoubleMetric](10).fn(10d, 9d) shouldBe true
    }

    "fail if metrics are not within X percent of each other" in {
      MetricComparator.withinXPercent[LongMetric](9.9).fn(10, 11) shouldBe false
      MetricComparator.withinXPercent[LongMetric](9.9).fn(10, 9) shouldBe false
      MetricComparator.withinXPercent[DoubleMetric](9.9).fn(10d, 9d) shouldBe false
      MetricComparator.withinXPercent[DoubleMetric](9.9).fn(10d, 11d) shouldBe false
    }
  }
}
