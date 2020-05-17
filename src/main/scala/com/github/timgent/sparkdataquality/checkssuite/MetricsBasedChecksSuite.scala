package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.CheckResult
import com.github.timgent.sparkdataquality.checks.QCCheck.{DualMetricBasedCheck, SingleMetricBasedCheck}
import com.github.timgent.sparkdataquality.metrics.{DatasetDescription, MetricDescriptor, MetricValue, MetricsCalculator}
import org.apache.spark.sql.Dataset
import cats.implicits._
import com.github.timgent.sparkdataquality.repository.{MetricsPersister, NullMetricsPersister}

import scala.concurrent.{ExecutionContext, Future}

case class DescribedDataset(dataset: Dataset[_], description: DatasetDescription)

object DescribedDataset {
  def apply(dataset: Dataset[_], datasetDescription: String): DescribedDataset =
    DescribedDataset(dataset, DatasetDescription(datasetDescription))
}

case class SingleDatasetMetricChecks(describedDataset: DescribedDataset, checks: Seq[SingleMetricBasedCheck[_]])

case class DualDatasetMetricChecks(describedDatasetA: DescribedDataset, describedDatasetB: DescribedDataset, checks: Seq[DualMetricBasedCheck[_]])

case class MetricsBasedChecksSuite(checkSuiteDescription: String,
                                   tags: Map[String, String],
                                   seqSingleDatasetMetricsChecks: Seq[SingleDatasetMetricChecks] = Seq.empty,
                                   seqDualDatasetMetricChecks: Seq[DualDatasetMetricChecks] = Seq.empty,
                                   metricsPersister: MetricsPersister = NullMetricsPersister,
                                   checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
                                  ) extends ChecksSuite {
  /**
   * Calculates the minimum required metrics to calculate this check suite
   */
  private def getMinimumRequiredMetrics(seqSingleDatasetMetricsChecks: Seq[SingleDatasetMetricChecks],
                                        seqDualDatasetMetricChecks: Seq[DualDatasetMetricChecks]
                                       ): Map[DescribedDataset, List[MetricDescriptor]] = {
    val singleDatasetMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] = (for {
      singleDatasetMetricChecks <- seqSingleDatasetMetricsChecks
      describedDataset: DescribedDataset = singleDatasetMetricChecks.describedDataset
      metricDescriptors = singleDatasetMetricChecks.checks.map(_.metricDescriptor).distinct.toList
    } yield (describedDataset, metricDescriptors)).toMap

    val dualDatasetAMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] = (for {
      dualDatasetMetricChecks <- seqDualDatasetMetricChecks
      describedDatasetA: DescribedDataset = dualDatasetMetricChecks.describedDatasetA
      metricDescriptors = dualDatasetMetricChecks.checks.map(_.dsAMetricDescriptor).distinct.toList
    } yield (describedDatasetA, metricDescriptors)).toMap

    val dualDatasetBMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] = (for {
      dualDatasetMetricChecks <- seqDualDatasetMetricChecks
      describedDatasetB: DescribedDataset = dualDatasetMetricChecks.describedDatasetB
      metricDescriptors = dualDatasetMetricChecks.checks.map(_.dsBMetricDescriptor).distinct.toList
    } yield (describedDatasetB, metricDescriptors)).toMap

    val allMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] =
      (singleDatasetMetricDescriptors |+| dualDatasetAMetricDescriptors |+| dualDatasetBMetricDescriptors).mapValues(_.distinct)

    allMetricDescriptors
  }

  override def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult] = {
    val allMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] =
      getMinimumRequiredMetrics(seqSingleDatasetMetricsChecks, seqDualDatasetMetricChecks)
    val calculatedMetrics: Map[DescribedDataset, Map[MetricDescriptor, MetricValue]] = allMetricDescriptors.map { case (describedDataset, metricDescriptors) =>
      val metricValues: Map[MetricDescriptor, MetricValue] = MetricsCalculator.calculateMetrics(describedDataset.dataset, metricDescriptors)
      (describedDataset, metricValues)
    }

    val storedMetricsFut = metricsPersister
      .storeMetrics(timestamp, calculatedMetrics.map{case (describedDataset, metrics) => (describedDataset.description, metrics)})

    for {
      _ <- storedMetricsFut
    } yield {
      val singleDatasetCheckResults: Seq[CheckResult] = seqSingleDatasetMetricsChecks.flatMap { singleDatasetMetricChecks =>
        val checks = singleDatasetMetricChecks.checks
        val datasetDescription = singleDatasetMetricChecks.describedDataset.description
        val metricsForDs: Map[MetricDescriptor, MetricValue] = calculatedMetrics(singleDatasetMetricChecks.describedDataset)
        val checkResults: Seq[CheckResult] = checks.map(_.applyCheckOnMetrics(metricsForDs).withDatasourceDescription(datasetDescription))
        checkResults
      }

      val dualDatasetCheckResults: Seq[CheckResult] = seqDualDatasetMetricChecks.flatMap { dualDatasetMetricChecks =>
        val checks = dualDatasetMetricChecks.checks
        val describedDatasetA = dualDatasetMetricChecks.describedDatasetA
        val describedDatasetB = dualDatasetMetricChecks.describedDatasetB
        val metricsForDsA: Map[MetricDescriptor, MetricValue] = calculatedMetrics(describedDatasetA)
        val metricsForDsB: Map[MetricDescriptor, MetricValue] = calculatedMetrics(describedDatasetB)
        val checkResults: Seq[CheckResult] = checks.map(_.applyCheckOnMetrics(metricsForDsA, metricsForDsB)
          .withDatasourceDescription(s"${describedDatasetA.description} compared to ${describedDatasetB.description}"))
        checkResults
      }

      val allCheckResults = singleDatasetCheckResults ++ dualDatasetCheckResults

      val overallCheckResult = checkResultCombiner(allCheckResults)
      ChecksSuiteResult(
        overallCheckResult,
        checkSuiteDescription,
        ChecksSuite.getOverallCheckResultDescription(allCheckResults),
        allCheckResults,
        timestamp,
        qcType,
        tags
      )
    }

  }

  override def qcType: QcType = QcType.MetricsBasedQualityCheck
}
