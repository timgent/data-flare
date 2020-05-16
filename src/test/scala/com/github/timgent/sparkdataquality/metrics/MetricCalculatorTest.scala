package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricCalculator.{SimpleMetricCalculator, SizeMetricCalculator}
import com.github.timgent.sparkdataquality.metrics.MetricValue.LongMetric
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import org.apache.spark.sql.Dataset

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
      val notAFilter = MetricFilter('str =!= "a", "letter not equal to a")
      testMetricAggFunction(ds, SizeMetricCalculator(notAFilter), LongMetric(2))
    }

    "return a failure when an invalid filter is given" in { // TODO: Implement error handling for bad metrics
      pending
    }
  }
}
