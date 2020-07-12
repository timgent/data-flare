package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckStatus, RawCheckResult} // DO NOT DELETE - required for tests
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SumValuesMetric // DO NOT DELETE - required for tests
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric} // DO NOT DELETE - required for tests
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SingleMetricCheckTest extends AnyWordSpec with Matchers {
  "SingleMetricCheck" should {
    "not compile" when {
      "the checks metric type doesn't match the metrics metric type" in {
        """
          |SingleMetricCheck[LongMetric](SumValuesMetric[DoubleMetric](""), "someCheck") { double: Long =>
          |  RawCheckResult(CheckStatus.Success, "someResultDescription")
          |}
          |""".stripMargin shouldNot compile
      }
    }
    "compile" when {
      "the checks metric type matches the metrics metric type" in {
        """
          |SingleMetricCheck[DoubleMetric](SumValuesMetric[DoubleMetric](""), "someCheck") { double: Long =>
          |  RawCheckResult(CheckStatus.Success, "someResultDescription")
          |}
          |""".stripMargin shouldNot compile
      }
    }
  }
}
