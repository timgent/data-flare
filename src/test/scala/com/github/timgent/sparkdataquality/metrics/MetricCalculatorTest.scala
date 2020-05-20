package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricCalculator.{ComplianceMetricCalculator, SimpleMetricCalculator, SizeMetricCalculator}
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricCalculatorTest extends AnyWordSpec with DatasetSuiteBase with Matchers {

  import spark.implicits._

  def testMetricAggFunction[T](ds: Dataset[_], calculator: SimpleMetricCalculator, expectedValue: T) = {
    calculator.valueFromRow(ds.agg(calculator.aggFunction).collect.head, 0) shouldBe expectedValue
  }

  "SizeMetricCalculator" should {
    lazy val ds = Seq(
      NumberString(1, "a"),
      NumberString(2, "b"),
      NumberString(3, "c")
    ).toDS

    "calculate the size of a DataFrame" in {
      testMetricAggFunction(ds, SizeMetricCalculator(MetricFilter.noFilter), LongMetric(3))
    }

    "apply the provided filter to count the size of matching rows in a DataFrame" in {
      val letterNotAFilter = MetricFilter('str =!= "a", "letter not equal to a")
      testMetricAggFunction(ds, SizeMetricCalculator(letterNotAFilter), LongMetric(2))
    }

    "return a failure when an invalid filter is given" in { // TODO: Implement error handling for bad metrics
      pending
    }
  }

  "ComplianceMetricCalculator" should {
    lazy val ds = Seq(
      NumberString(1, "a"),
      NumberString(2, "b"),
      NumberString(3, "c"),
      NumberString(4, "d"),
      NumberString(5, "e"),
      NumberString(6, "f"),
      NumberString(7, "g"),
      NumberString(8, "h"),
      NumberString(9, "i"),
      NumberString(10, "j")
    ).toDS

    "calculate the compliance rate of a DataFrame correctly" in {
      testMetricAggFunction(ds, ComplianceMetricCalculator(ComplianceFn(col("number") >= 7, "Number >= 6"),
        MetricFilter.noFilter), DoubleMetric(0.4))
    }

    "apply the provided filter to count the size of matching rows in a DataFrame" in {
      testMetricAggFunction(ds,
        ComplianceMetricCalculator(ComplianceFn(col("number") >= 7, "Number >= 7"),
          MetricFilter(col("number") >= 6, "number >= 6")),
        DoubleMetric(0.8))
    }

    "return a failure when an invalid filter is given" in { // TODO: Implement error handling for bad metrics
      pending
    }

    "return a failure when an invalid compliance fn is given" in { // TODO: Implement error handling for bad metrics
      pending
    }
  }
}
