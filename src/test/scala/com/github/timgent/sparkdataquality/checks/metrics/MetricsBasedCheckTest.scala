package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.metrics.SingleMetricBasedCheck.{ComplianceCheck, SizeCheck}
import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.sparkdataquality.metrics.{MetricComparator, MetricDescriptor, MetricFilter}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsBasedCheckTest extends AnyWordSpec with DatasetSuiteBase with Matchers {

  "DualMetricBasedCheck" should {
    val simpleSizeMetric = MetricDescriptor.SizeMetricDescriptor()
    val dualMetricBasedCheck =
      DualMetricBasedCheck[LongMetric](simpleSizeMetric, simpleSizeMetric, "size comparison")(
        MetricComparator.metricsAreEqual
      )
    "pass the check when the required metrics are provided in the metrics map and they meet the comparator criteria" in {
      val checkResult: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(
        Map(simpleSizeMetric -> LongMetric(2L)),
        Map(simpleSizeMetric -> LongMetric(2L))
      )
      checkResult shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Success,
        "metric comparison passed. dsAMetric of LongMetric(2) was compared to dsBMetric of LongMetric(2)",
        "size comparison. Comparing metric SimpleMetricDescriptor(Size,Some(no filter),None,None) to SizeMetricDescriptor(MetricFilter(true,no filter)) using comparator of metrics are equal"
      )
    }

    "fail the check when the required metrics are provided in the metrics map but they do not meet the comparator criteria" in {
      val checkResult: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(
        Map(simpleSizeMetric -> LongMetric(2L)),
        Map(simpleSizeMetric -> LongMetric(3L))
      )
      checkResult shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Error,
        "metric comparison failed. dsAMetric of LongMetric(2) was compared to dsBMetric of LongMetric(3)",
        "size comparison. Comparing metric SimpleMetricDescriptor(Size,Some(no filter),None,None) to SizeMetricDescriptor(MetricFilter(true,no filter)) using comparator of metrics are equal"
      )
    }

    "fail when the required metrics are not provided in the metrics map" in {
      val result: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(Map.empty, Map.empty)
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Failed to find corresponding metric for this check. Please report this error - this should not occur"
    }

    "fail when the required metrics are the wrong type" in {
      val result: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(
        Map(simpleSizeMetric -> DoubleMetric(2)),
        Map(simpleSizeMetric -> DoubleMetric(2))
      )
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Found metric of the wrong type for this check. Please report this error - this should not occur"
    }
  }

  "SingleMetricBasedCheck for any check type" should {
    val simpleSizeCheck = SizeCheck(AbsoluteThreshold(Some(0), Some(3)), MetricFilter.noFilter)

    "apply the check when the required metric is provided in the metrics map" in {
      val result =
        simpleSizeCheck.applyCheckOnMetrics(Map(simpleSizeCheck.metricDescriptor -> LongMetric(2)))
      result.status shouldBe CheckStatus.Success
    }

    "fail when the required metric is not provided in the metrics map" in {
      val result = simpleSizeCheck.applyCheckOnMetrics(Map.empty)
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Failed to find corresponding metric for this check. Please report this error - this should not occur"
    }

    "fail when the required metric is the wrong type" in {
      val result = simpleSizeCheck.applyCheckOnMetrics(
        Map(simpleSizeCheck.metricDescriptor -> DoubleMetric(2))
      )
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Found metric of the wrong type for this check. Please report this error - this should not occur"
    }
  }

  "MetricsBasedCheck.SizeCheck" should {
    "pass a check where the size is within the threshold" in {
      val check = SizeCheck(AbsoluteThreshold(Some(0), Some(3)), MetricFilter.noFilter)
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metricDescriptor -> LongMetric(2)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Success,
        "Size of 2 was within the range between 0 and 3",
        "SizeCheck with filter: no filter"
      )
    }

    "fail a check where the size is outside the threshold" in {
      val check =
        SizeCheck(AbsoluteThreshold(Some(0), Some(3)), MetricFilter(lit(false), "someFilter"))
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metricDescriptor -> LongMetric(4)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Error,
        "Size of 4 was outside the range between 0 and 3",
        "SizeCheck with filter: someFilter"
      )
    }
  }

  "MetricsBasedCheck.ComplianceCheck" should {
    "pass a check where the compliance rate is within the threshold" in {
      val check = ComplianceCheck(
        AbsoluteThreshold(Some(0.9), Some(1)),
        someComplianceFn,
        MetricFilter.noFilter
      )
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metricDescriptor -> DoubleMetric(0.9)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Success,
        "Compliance of 0.9 was within the range between 0.9 and 1.0",
        "ComplianceCheck someComplianceFn with filter: no filter"
      )
    }

    "fail a check where the compliance rate is outside the threshold" in {
      val check = ComplianceCheck(
        AbsoluteThreshold(Some(0.9), Some(1)),
        someComplianceFn,
        MetricFilter.noFilter
      )
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metricDescriptor -> DoubleMetric(0.8)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Error,
        "Compliance of 0.8 was outside the range between 0.9 and 1.0",
        "ComplianceCheck someComplianceFn with filter: no filter"
      )
    }
  }
}
