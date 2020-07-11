package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.DatasourceDescription.DualDsDescription
import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType, RawCheckResult}
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.sparkdataquality.metrics.{MetricComparator, MetricDescriptor, MetricFilter}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.functions.lit
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsBasedCheckTest extends AnyWordSpec with DatasetSuiteBase with Matchers with MockFactory {

  "DualMetricBasedCheck" should {
    val simpleSizeMetric = MetricDescriptor.SizeMetric()
    val dualMetricBasedCheck =
      DualMetricCheck[LongMetric](simpleSizeMetric, simpleSizeMetric, "size comparison",
        MetricComparator.metricsAreEqual
      )
    val dualDsDescription = DualDsDescription("dsA", "dsB")
    "pass the check when the required metrics are provided in the metrics map and they meet the comparator criteria" in {
      val checkResult: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(
        Map(simpleSizeMetric -> LongMetric(2L)),
        Map(simpleSizeMetric -> LongMetric(2L)),
        dualDsDescription
      )
      checkResult shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Success,
        "metric comparison passed. dsA with LongMetric(2) was compared to dsB with LongMetric(2)",
        "size comparison. Comparing metric SimpleMetricDescriptor(Size,Some(no filter),None,None) to SimpleMetricDescriptor(Size,Some(no filter),None,None) using comparator of metrics are equal"
      )
    }

    "fail the check when the required metrics are provided in the metrics map but they do not meet the comparator criteria" in {
      val checkResult: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(
        Map(simpleSizeMetric -> LongMetric(2L)),
        Map(simpleSizeMetric -> LongMetric(3L)),
        dualDsDescription
      )
      checkResult shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Error,
        "metric comparison failed. dsA with LongMetric(2) was compared to dsB with LongMetric(3)",
        "size comparison. Comparing metric SimpleMetricDescriptor(Size,Some(no filter),None,None) to SimpleMetricDescriptor(Size,Some(no filter),None,None) using comparator of metrics are equal"
      )
    }

    "fail when the required metrics are not provided in the metrics map" in {
      val result: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(Map.empty, Map.empty, dualDsDescription)
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Failed to find corresponding metric for this check. Please report this error - this should not occur"
    }

    "fail when the required metrics are the wrong type" in {
      val result: CheckResult = dualMetricBasedCheck.applyCheckOnMetrics(
        Map(simpleSizeMetric -> DoubleMetric(2)),
        Map(simpleSizeMetric -> DoubleMetric(2)),
        dualDsDescription
      )
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Found metric of the wrong type for this check. Please report this error - this should not occur"
    }
  }

  // TODO: Remove "New" prefix and replace all existing SingleMetricBasedChecks with this one
  "NewSingleMetricsBasedCheck" should {
    val metric = mock[MetricDescriptor]
    val exampleCheck = SingleMetricCheck[LongMetric](metric, "exampleCheck") { metricValue =>
      val isWithinThreshold = AbsoluteThreshold(2L, 2L).isWithinThreshold(metricValue)
      if (isWithinThreshold)
        RawCheckResult(CheckStatus.Success, "it's within the threshold")
      else
        RawCheckResult(CheckStatus.Error, "It's not in the threshold")
    }

    "apply the check when the required metric is provided in the metrics map" in {
      val result =
        exampleCheck.applyCheckOnMetrics(Map(exampleCheck.metric -> LongMetric(2)))
      result.status shouldBe CheckStatus.Success
    }

    "fail when the required metric is not provided in the metrics map" in {
      val result = exampleCheck.applyCheckOnMetrics(Map.empty)
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Failed to find corresponding metric for this check. Please report this error - this should not occur"
    }

    "fail when the required metric is the wrong type" in {
      val result = exampleCheck.applyCheckOnMetrics(
        Map(exampleCheck.metric -> DoubleMetric(2))
      )
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Found metric of the wrong type for this check. Please report this error - this should not occur"
    }
  }

  "SingleMetricBasedCheck for any check type" should {
    val simpleSizeCheck = SingleMetricCheck.sizeCheck(AbsoluteThreshold(Some(0L), Some(3L)), MetricFilter.noFilter)

    "apply the check when the required metric is provided in the metrics map" in {
      val result =
        simpleSizeCheck.applyCheckOnMetrics(Map(simpleSizeCheck.metric -> LongMetric(2)))
      result.status shouldBe CheckStatus.Success
    }

    "fail when the required metric is not provided in the metrics map" in {
      val result = simpleSizeCheck.applyCheckOnMetrics(Map.empty)
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Failed to find corresponding metric for this check. Please report this error - this should not occur"
    }

    "fail when the required metric is the wrong type" in {
      val result = simpleSizeCheck.applyCheckOnMetrics(
        Map(simpleSizeCheck.metric -> DoubleMetric(2))
      )
      result.status shouldBe CheckStatus.Error
      result.resultDescription shouldBe "Found metric of the wrong type for this check. Please report this error - this should not occur"
    }
  }

  "SingleMetricBasedCheck.sizeCheck" should {
    "pass a check where the size is within the threshold" in {
      val check = SingleMetricCheck.sizeCheck(AbsoluteThreshold(Some(0L), Some(3L)), MetricFilter.noFilter)
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metric -> LongMetric(2)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Success,
        "Size of 2 was within the range between 0 and 3",
        "SizeCheck with filter: no filter"
      )
    }

    "fail a check where the size is outside the threshold" in {
      val check =
        SingleMetricCheck.sizeCheck(AbsoluteThreshold(Some(0L), Some(3L)), MetricFilter(lit(false), "someFilter"))
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metric -> LongMetric(4)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Error,
        "Size of 4 was outside the range between 0 and 3",
        "SizeCheck with filter: someFilter"
      )
    }
  }

  "SingleMetricBasedCheck.complianceCheck" should {
    "pass a check where the compliance rate is within the threshold" in {
      val check = SingleMetricCheck.complianceCheck(
        AbsoluteThreshold(Some(0.9), Some(1d)),
        someComplianceFn,
        MetricFilter.noFilter
      )
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metric -> DoubleMetric(0.9)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Success,
        "Compliance of 0.9 was within the range between 0.9 and 1.0",
        "ComplianceCheck someComplianceFn with filter: no filter"
      )
    }

    "fail a check where the compliance rate is outside the threshold" in {
      val check = SingleMetricCheck.complianceCheck(
        AbsoluteThreshold(Some(0.9), Some(1d)),
        someComplianceFn,
        MetricFilter.noFilter
      )
      val result: CheckResult =
        check.applyCheckOnMetrics(Map(check.metric -> DoubleMetric(0.8)))
      result shouldBe CheckResult(
        QcType.MetricsBasedQualityCheck,
        CheckStatus.Error,
        "Compliance of 0.8 was outside the range between 0.9 and 1.0",
        "ComplianceCheck someComplianceFn with filter: no filter"
      )
    }
  }
}
