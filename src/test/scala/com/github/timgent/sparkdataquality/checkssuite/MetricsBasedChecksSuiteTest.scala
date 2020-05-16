package com.github.timgent.sparkdataquality.checkssuite

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import com.github.timgent.sparkdataquality.checks.QCCheck.MetricsBasedCheck
import com.github.timgent.sparkdataquality.metrics.{MetricDescriptor, MetricFilter}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsBasedChecksSuiteTest extends AnyWordSpec with DatasetSuiteBase with Matchers {
  import spark.implicits._
  "MetricsBasedChecksSuite" should {
    "correctly calculate metrics based checks" in {
      val ds = Seq(
        NumberString(1, "a"),
        NumberString(2, "b")
      )
      val checks: Seq[MetricsBasedCheck[_, _]] = Seq(
        MetricsBasedCheck.SizeCheck(AbsoluteThreshold(Some(2), Some(2)), MetricFilter.noFilter)
      )
      val checkSuiteDescription = "my first metricsCheckSuite"
      val metricsBasedChecksSuite = MetricsBasedChecksSuite(ds.toDS, checks, checkSuiteDescription, someTags)

      val checkResults: ChecksSuiteResult = metricsBasedChecksSuite.run(now)
      checkResults shouldBe ChecksSuiteResult(
        CheckSuiteStatus.Success,
        checkSuiteDescription,
        "1 checks were successful. 0 checks gave errors. 0 checks gave warnings",
        Seq(CheckResult(
          CheckStatus.Success, "Size of 2 was within the range between 2 and 2", "SizeCheck with filter: no filter"
        )),
        now,
        QcType.MetricsBasedQualityCheck,
        someTags
      )
    }
    "store metrics in a metrics repository" in {
      pending
    }
  }
}
