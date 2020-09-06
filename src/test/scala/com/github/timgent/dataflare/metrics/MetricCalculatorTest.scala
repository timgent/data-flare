package com.github.timgent.dataflare.metrics

import com.github.timgent.dataflare.metrics.MetricCalculator.{
  ComplianceMetricCalculator,
  DistinctValuesMetricCalculator,
  DistinctnessMetricCalculator,
  SimpleMetricCalculator,
  SizeMetricCalculator,
  SumValuesMetricCalculator
}
import com.github.timgent.dataflare.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.dataflare.utils.CommonFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricCalculatorTest extends AnyWordSpec with DatasetSuiteBase with Matchers {

  import spark.implicits._

  def testMetricAggFunction[T](
      ds: Dataset[_],
      calculator: SimpleMetricCalculator,
      expectedValue: T
  ) = {
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
      testMetricAggFunction(
        ds,
        ComplianceMetricCalculator(
          ComplianceFn(col("number") >= 7, "Number >= 6"),
          MetricFilter.noFilter
        ),
        DoubleMetric(0.4)
      )
    }

    "apply the provided filter before calculating the compliance in a DataFrame" in {
      testMetricAggFunction(
        ds,
        ComplianceMetricCalculator(
          ComplianceFn(col("number") <= 9, "Number <= 9"),
          MetricFilter(col("number") >= 9, "number >= 9")
        ),
        DoubleMetric(0.5)
      )
    }
  }

  "DistinctValuesMetricCalculator" should {
    lazy val ds = Seq(
      NumberString(1, "a"),
      NumberString(2, "b"),
      NumberString(3, "c"),
      NumberString(3, "d"),
      NumberString(3, "d") // not distinct
    ).toDS

    "calculate the number of distinct values for a set of columns correctly" in {
      testMetricAggFunction(
        ds,
        DistinctValuesMetricCalculator(List("number", "str"), MetricFilter.noFilter),
        LongMetric(4)
      )
    }

    "apply the provided filter before calculating the distinct values in a DataFrame" in {
      testMetricAggFunction(
        ds,
        DistinctValuesMetricCalculator(
          List("number", "str"),
          MetricFilter(col("str") =!= "c", "string not equal to c")
        ),
        LongMetric(3)
      )
    }
  }

  "DistinctnessMetricCalculator" should {
    lazy val ds = Seq(
      NumberString(1, "a"),
      NumberString(2, "b"),
      NumberString(3, "c"),
      NumberString(3, "d"),
      NumberString(3, "d") // not distinct
    ).toDS

    "calculate the distinctness for a set of columns correctly" in {
      testMetricAggFunction(
        ds,
        DistinctnessMetricCalculator(List("number", "str"), MetricFilter.noFilter),
        DoubleMetric(4d / 5d)
      )
    }

    "apply the provided filter before calculating the compliance in a DataFrame" in {
      testMetricAggFunction(
        ds,
        DistinctnessMetricCalculator(
          List("number", "str"),
          MetricFilter(col("str") =!= "c", "string not equal to c")
        ),
        DoubleMetric(3d / 4d)
      )
    }
  }

  "SumValuesMetricCalculator" should {
    lazy val ds = Seq(
      NumberString(1, "a"),
      NumberString(2, "b"),
      NumberString(3, "c"),
      NumberString(3, "d"),
      NumberString(3, "d") // not distinct
    ).toDS

    "calculate the sum of values for a given column" in {
      testMetricAggFunction(
        ds,
        SumValuesMetricCalculator[LongMetric]("number", MetricFilter.noFilter),
        LongMetric(12)
      )
    }

    "apply the provided filter before calculating the compliance in a DataFrame" in {
      testMetricAggFunction(
        ds,
        SumValuesMetricCalculator[LongMetric]("number", MetricFilter(col("str") =!= lit("d"), "not d")),
        LongMetric(6)
      )
    }
  }
}
