package com.github.timgent.dataflare.checks.metrics

import java.time.Instant

import com.github.timgent.dataflare.checks.CheckDescription.SingleMetricCheckDescription
import com.github.timgent.dataflare.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.dataflare.metrics.MetricDescriptor.SizeMetric
import com.github.timgent.dataflare.metrics.MetricValue.LongMetric
import com.github.timgent.dataflare.metrics.SimpleMetricDescriptor
import com.github.timgent.dataflare.utils.CommonFixtures.now
import com.github.timgent.dataflare.utils.DateTimeUtils.InstantExtension
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SingleMetricAnomalyCheckTest extends AnyWordSpec with Matchers {
  "stdChangeAnomalyCheck" should {
    "anomaly STD check" in {
      val check: SingleMetricAnomalyCheck[LongMetric] =
        SingleMetricAnomalyCheck.stdChangeAnomalyCheck(2, 2, SizeMetric())
      val metric: LongMetric = LongMetric(60)
      val historicMetrics: Map[Instant, Long] =
        Map(
          now -> 50,
          now.plusDays(1) -> 60,
          now.plusDays(2) -> 70
        )

      val result: CheckResult = check.applyCheck(metric, historicMetrics)

      result shouldBe CheckResult(
        QcType.SingleMetricAnomalyCheck,
        CheckStatus.Success,
        "MetricValue of 60 was not anomalous compared to previous results. Mean: 60.0; STD: 8.16496580927726",
        SingleMetricCheckDescription("STDChangeAnomalyCheck", SimpleMetricDescriptor("Size", Some("no filter"))),
        None
      )
    }

    "outlier should be treated as an anomaly" in {
      val check: SingleMetricAnomalyCheck[LongMetric] =
        SingleMetricAnomalyCheck.stdChangeAnomalyCheck(2, 2, SizeMetric())
      val metric: LongMetric = LongMetric(35)
      val historicMetrics: Map[Instant, Long] =
        Map(
          now -> 50,
          now.plusDays(1) -> 60,
          now.plusDays(2) -> 70
        )

      val result: CheckResult = check.applyCheck(metric, historicMetrics)

      result shouldBe CheckResult(
        QcType.SingleMetricAnomalyCheck,
        CheckStatus.Error,
        "MetricValue of 35 was anomalous compared to previous results. Mean: 60.0; STD: 8.16496580927726",
        SingleMetricCheckDescription("STDChangeAnomalyCheck", SimpleMetricDescriptor("Size", Some("no filter"))),
        None
      )
    }
  }
}
