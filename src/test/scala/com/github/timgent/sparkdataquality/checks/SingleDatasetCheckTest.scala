package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.SingleDatasetCheck.sumOfValuesCheck
import com.github.timgent.sparkdataquality.checks.metrics.DualMetricBasedCheck
import com.github.timgent.sparkdataquality.checks.metrics.SingleMetricBasedCheck.SizeCheck
import com.github.timgent.sparkdataquality.checkssuite.DescribedDataset
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.sparkdataquality.metrics.{MetricComparator, MetricDescriptor, MetricFilter}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import com.github.timgent.sparkdataquality.utils.TestDataClass
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SingleDatasetCheckTest extends AnyWordSpec with DatasetSuiteBase with Matchers {

  import spark.implicits._

  "sumOfValuesCheck" should {
    val columnName = "number"
    lazy val dsWithNumberSumOf6 =
      DescribedDataset(List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS, "dsWithNumberSumOf6")

    def expectedResultDescription(passed: Boolean, threshold: AbsoluteThreshold[Long]) = if (passed)
      s"Sum of column number was 6, which was within the threshold $threshold"
    else
      s"Sum of column number was 6, which was outside the threshold $threshold"
    "pass the qc check" when {
      "sum of values is above a lower bound" in {
        val threshold = AbsoluteThreshold(Some(5L), None)
        val result: CheckResult = sumOfValuesCheck(columnName, threshold).applyCheck(dsWithNumberSumOf6)
        result.status shouldBe CheckStatus.Success
        result.resultDescription shouldBe expectedResultDescription(passed = true, threshold)
      }

      "sum of values is below an upper bound" in {
        val threshold = AbsoluteThreshold(None, Some(7L))
        val result: CheckResult = sumOfValuesCheck(columnName, threshold).applyCheck(dsWithNumberSumOf6)
        result.status shouldBe CheckStatus.Success
        result.resultDescription shouldBe expectedResultDescription(passed = true, threshold)
      }

      "sum of values is within both bounds" in {
        val threshold = AbsoluteThreshold(Some(5L), Some(7L))
        val result: CheckResult = sumOfValuesCheck(columnName, threshold).applyCheck(dsWithNumberSumOf6)
        result.status shouldBe CheckStatus.Success
        result.resultDescription shouldBe expectedResultDescription(passed = true, threshold)
      }
    }

    "fail the qc check" when {
      "sum of values is below a lower bound" in {
        val threshold = AbsoluteThreshold(Some(7L), None)
        val result: CheckResult = sumOfValuesCheck(columnName, threshold).applyCheck(dsWithNumberSumOf6)
        result.status shouldBe CheckStatus.Error
        result.resultDescription shouldBe expectedResultDescription(passed = false, threshold)
      }
      "sum of values is above an upper bound" in {
        val threshold = AbsoluteThreshold(None, Some(5L))
        val result: CheckResult = sumOfValuesCheck(columnName, threshold).applyCheck(dsWithNumberSumOf6)
        result.status shouldBe CheckStatus.Error
        result.resultDescription shouldBe expectedResultDescription(passed = false, threshold)
      }
    }

  }
}
