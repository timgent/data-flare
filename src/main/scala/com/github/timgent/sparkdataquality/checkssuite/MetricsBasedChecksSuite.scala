package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import cats.implicits._
import com.github.timgent.sparkdataquality.checks.CheckResult
import com.github.timgent.sparkdataquality.checks.metrics.{DualMetricBasedCheck, SingleMetricBasedCheck}
import com.github.timgent.sparkdataquality.metrics.{DatasetDescription, MetricDescriptor, MetricValue, MetricsCalculator}
import com.github.timgent.sparkdataquality.repository.{MetricsPersister, NullMetricsPersister}
import org.apache.spark.sql.Dataset

import scala.concurrent.{ExecutionContext, Future}

/**
 * A dataset with description
 * @param ds - the dataset
 * @param description - description of the dataset
 */
case class DescribedDataset(ds: Dataset[_], description: DatasetDescription)

object DescribedDataset {
  def apply(dataset: Dataset[_], datasetDescription: String): DescribedDataset =
    DescribedDataset(dataset, DatasetDescription(datasetDescription))
}

/**
 * List of SingleMetricBasedChecks to be run on a single dataset
 * @param describedDataset - the dataset the checks are being run on
 * @param checks - a list of checks to be run
 */
case class SingleDatasetMetricChecks(describedDataset: DescribedDataset, checks: Seq[SingleMetricBasedCheck[_]] = Seq.empty) {
  def withChecks(checks: Seq[SingleMetricBasedCheck[_]]) = this.copy(checks = this.checks ++ checks)
}

/**
 * List of DualMetricBasedChecks to be run on a pair of datasets
 * @param describedDatasetA - the first dataset that is part of the comparison
 * @param describedDatasetB - the second dataset that is part of the comparison
 * @param checks - the checks to be done on the datasets
 */
case class DualDatasetMetricChecks(describedDatasetA: DescribedDataset, describedDatasetB: DescribedDataset,
                                   checks: Seq[DualMetricBasedCheck[_]])

/**
 * A ChecksSuite that can run checks based on metrics (which are computed more efficiently than arbitrary checks).
 * @param checkSuiteDescription - Description of the CheckSuite
 * @param tags - the tags associated with the CheckSuite
 * @param seqSingleDatasetMetricsChecks - checks to be performed on individual datasets
 * @param seqDualDatasetMetricChecks - checks to be performed on pairs of datasets
 * @param metricsPersister - responsible for persisting metrics
 * @param checkResultCombiner - determines how overall CheckSuiteStatus will be calculated given the results of
 *                            individual checks
 */
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
      metricDescriptors = singleDatasetMetricChecks.checks.map(_.metricDescriptor).toList
    } yield (describedDataset, metricDescriptors)).groupBy(_._1).mapValues(_.flatMap(_._2).toList)

    val dualDatasetAMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] = (for {
      dualDatasetMetricChecks <- seqDualDatasetMetricChecks
      describedDatasetA: DescribedDataset = dualDatasetMetricChecks.describedDatasetA
      metricDescriptors = dualDatasetMetricChecks.checks.map(_.dsAMetricDescriptor).toList
    } yield (describedDatasetA, metricDescriptors)).groupBy(_._1).mapValues(_.flatMap(_._2).toList)

    val dualDatasetBMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] = (for {
      dualDatasetMetricChecks <- seqDualDatasetMetricChecks
      describedDatasetB: DescribedDataset = dualDatasetMetricChecks.describedDatasetB
      metricDescriptors = dualDatasetMetricChecks.checks.map(_.dsBMetricDescriptor).toList
    } yield (describedDatasetB, metricDescriptors)).groupBy(_._1).mapValues(_.flatMap(_._2).toList)

    val allMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] =
      (singleDatasetMetricDescriptors |+| dualDatasetAMetricDescriptors |+| dualDatasetBMetricDescriptors).mapValues(_.distinct)

    allMetricDescriptors
  }

  override def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult] = {
    val allMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] =
      getMinimumRequiredMetrics(seqSingleDatasetMetricsChecks, seqDualDatasetMetricChecks)
    val calculatedMetrics: Map[DescribedDataset, Map[MetricDescriptor, MetricValue]] = allMetricDescriptors.map { case (describedDataset, metricDescriptors) =>
      val metricValues: Map[MetricDescriptor, MetricValue] = MetricsCalculator.calculateMetrics(describedDataset.ds, metricDescriptors)
      (describedDataset, metricValues)
    }

    val metricsToSave = calculatedMetrics.map { case (describedDataset, metrics) =>
      (describedDataset.description, metrics.map{ case (descriptor, value) => (descriptor.toSimpleMetricDescriptor, value)})
    }
    val storedMetricsFut = metricsPersister.save(timestamp, metricsToSave)

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
