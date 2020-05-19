package com.github.timgent.sparkdataquality.checkssuite

import com.github.timgent.sparkdataquality.checks.QCCheck.{DualMetricBasedCheck, SingleMetricBasedCheck}
import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus}
import com.github.timgent.sparkdataquality.metrics.MetricValue.LongMetric
import com.github.timgent.sparkdataquality.metrics.{DatasetDescription, MetricComparator, MetricDescriptor, MetricFilter}
import com.github.timgent.sparkdataquality.repository.InMemoryMetricsPersister
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class MetricsBasedChecksSuiteTest extends AsyncWordSpec with DatasetSuiteBase with Matchers {

  import spark.implicits._

  "MetricsBasedChecksSuite" should {
    "calculate metrics based checks on single datasets" in {
      val ds = Seq(
        NumberString(1, "a"),
        NumberString(2, "b")
      ).toDS
      val checks: Seq[SingleDatasetMetricChecks] = Seq(SingleDatasetMetricChecks(
        DescribedDataset(ds, datasourceDescription),
        Seq(SingleMetricBasedCheck.SizeCheck(AbsoluteThreshold(Some(2), Some(2)), MetricFilter.noFilter))
      ))
      val checkSuiteDescription = "my first metricsCheckSuite"
      val metricsBasedChecksSuite = MetricsBasedChecksSuite(checkSuiteDescription, someTags, checks)

      for {
        checkResults: ChecksSuiteResult <- metricsBasedChecksSuite.run(now)
      } yield {
        checkResults shouldBe ChecksSuiteResult(
          CheckSuiteStatus.Success,
          checkSuiteDescription,
          "1 checks were successful. 0 checks gave errors. 0 checks gave warnings",
          Seq(CheckResult(
            CheckStatus.Success, "Size of 2 was within the range between 2 and 2", "SizeCheck with filter: no filter", Some(datasourceDescription)
          )),
          now,
          QcType.MetricsBasedQualityCheck,
          someTags
        )
      }
    }

    "calculate single dataset metrics checks across a number of datasets" in {
      val dsA = Seq(
        NumberString(1, "a"),
        NumberString(2, "b")
      )
      val dsB = Seq(
        NumberString(1, "a"),
        NumberString(2, "b"),
        NumberString(3, "c")
      )
      val checks: Seq[SingleMetricBasedCheck[_]] = Seq(
        SingleMetricBasedCheck.SizeCheck(AbsoluteThreshold(Some(2), Some(2)), MetricFilter.noFilter)
      )
      val singleDatasetChecks = Seq(
        SingleDatasetMetricChecks(DescribedDataset(dsA.toDS, "dsA"), checks),
        SingleDatasetMetricChecks(DescribedDataset(dsB.toDS, "dsB"), checks)
      )
      val checkSuiteDescription = "my first metricsCheckSuite"
      val metricsBasedChecksSuite = MetricsBasedChecksSuite(checkSuiteDescription, someTags, singleDatasetChecks)

      for {
        checkResults: ChecksSuiteResult <- metricsBasedChecksSuite.run(now)
      } yield {
        checkResults shouldBe ChecksSuiteResult(
          CheckSuiteStatus.Error,
          checkSuiteDescription,
          "1 checks were successful. 1 checks gave errors. 0 checks gave warnings",
          Seq(
            CheckResult(CheckStatus.Success, "Size of 2 was within the range between 2 and 2", "SizeCheck with filter: no filter", Some("dsA")),
            CheckResult(CheckStatus.Error, "Size of 3 was outside the range between 2 and 2", "SizeCheck with filter: no filter", Some("dsB"))
          ),
          now,
          QcType.MetricsBasedQualityCheck,
          someTags
        )
      }
    }

    "calculate metrics based checks between datasets" in {
      val simpleSizeMetric = MetricDescriptor.SizeMetricDescriptor()
      val dsA = Seq(
        NumberString(1, "a"),
        NumberString(2, "b"),
        NumberString(3, "c")
      ).toDS
      val dsB = Seq(
        NumberString(1, "a"),
        NumberString(2, "b"),
        NumberString(3, "c")
      ).toDS
      val metricChecks = Seq(
        DualMetricBasedCheck(simpleSizeMetric, simpleSizeMetric, MetricComparator.metricsAreEqual, "check size metrics are equal")
      )
      val dualDatasetChecks = DualDatasetMetricChecks(
        DescribedDataset(dsA, "dsA"),
        DescribedDataset(dsB, "dsB"),
        metricChecks
      )
      val checkSuiteDescription = "my first metricsCheckSuite"
      val metricsBasedChecksSuite = MetricsBasedChecksSuite(checkSuiteDescription, someTags, Seq.empty, Seq(dualDatasetChecks))

      for {
        checkResults: ChecksSuiteResult <- metricsBasedChecksSuite.run(now)
      } yield {
        checkResults shouldBe ChecksSuiteResult(
          CheckSuiteStatus.Success,
          checkSuiteDescription,
          "1 checks were successful. 0 checks gave errors. 0 checks gave warnings",
          Seq(CheckResult(
            CheckStatus.Success, "metric comparison passed", "check size metrics are equal", Some("dsA compared to dsB")
          )),
          now,
          QcType.MetricsBasedQualityCheck,
          someTags
        )
      }
    }

    "store all metrics in a metrics repository" in {
      val simpleSizeMetric = MetricDescriptor.SizeMetricDescriptor()
      val dsA = Seq(
        NumberString(1, "a"),
        NumberString(2, "b"),
        NumberString(3, "c")
      ).toDS
      val dsB = Seq(
        NumberString(1, "a"),
        NumberString(2, "b")
      ).toDS
      val dsC = Seq(
        NumberString(1, "a")
      ).toDS
      val dualMetricChecks = Seq(
        DualMetricBasedCheck(simpleSizeMetric, simpleSizeMetric, MetricComparator.metricsAreEqual, "check size metrics are equal")
      )
      val dualDatasetChecks = DualDatasetMetricChecks(
        DescribedDataset(dsA, "dsA"),
        DescribedDataset(dsB, "dsB"),
        dualMetricChecks
      )
      val singleDatasetChecks: Seq[SingleDatasetMetricChecks] = Seq(SingleDatasetMetricChecks(
        DescribedDataset(dsC, "dsC"),
        Seq(SingleMetricBasedCheck.SizeCheck(AbsoluteThreshold(Some(2), Some(2)), MetricFilter.noFilter))
      ))
      val checkSuiteDescription = "my first metricsCheckSuite"
      val inMemoryMetricsPersister = new InMemoryMetricsPersister
      val metricsBasedChecksSuite = MetricsBasedChecksSuite(checkSuiteDescription, someTags, singleDatasetChecks,
        Seq(dualDatasetChecks), inMemoryMetricsPersister)

      for {
        _ <- metricsBasedChecksSuite.run(now)
        storedMetrics <- inMemoryMetricsPersister.loadAll
      } yield storedMetrics shouldBe Map(
        now -> Map(
          DatasetDescription("dsA") -> Map(simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(3L)),
          DatasetDescription("dsB") -> Map(simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(2L)),
          DatasetDescription("dsC") -> Map(simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(1L))
        )
      )
    }
  }
}
