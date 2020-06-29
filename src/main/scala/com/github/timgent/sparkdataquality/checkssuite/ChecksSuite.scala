package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import cats.implicits._
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import com.github.timgent.sparkdataquality.checks.DatasetComparisonCheck.DatasetPair
import com.github.timgent.sparkdataquality.checks._
import com.github.timgent.sparkdataquality.checks.metrics.{DualMetricBasedCheck, SingleMetricBasedCheck}
import com.github.timgent.sparkdataquality.deequ.DeequHelpers.VerificationResultExtension
import com.github.timgent.sparkdataquality.deequ.DeequNullMetricsRepository
import com.github.timgent.sparkdataquality.metrics.{DatasetDescription, MetricDescriptor, MetricValue, MetricsCalculator}
import com.github.timgent.sparkdataquality.repository.{
  InMemoryQcResultsRepository,
  MetricsPersister,
  NullMetricsPersister,
  NullQcResultsRepository,
  QcResultsRepository
}
import com.github.timgent.sparkdataquality.sparkdataquality.DeequMetricsRepository
import org.apache.spark.sql.Dataset

import scala.concurrent.{ExecutionContext, Future}

case class SingleDatasetCheckWithDs(dataset: DescribedDataset, checks: Seq[SingleDatasetCheck])

case class DatasetComparisonCheckWithDs(
    datasets: DescribedDatasetPair,
    checks: Seq[DatasetComparisonCheck]
)

case class DescribedDatasetPair(dataset: DescribedDataset, datasetToCompare: DescribedDataset) {
  def datasourceDescription: Option[String] =
    Some(s"dataset: ${dataset.description}. datasetToCompare: ${datasetToCompare.description}")

  private[sparkdataquality] def rawDatasetPair = DatasetPair(dataset.ds, datasetToCompare.ds)
}

case class DeequCheck(describedDataset: DescribedDataset, checks: Seq[DeequQCCheck])

/**
  * List of SingleMetricBasedChecks to be run on a single dataset
  * @param describedDataset - the dataset the checks are being run on
  * @param checks - a list of checks to be run
  */
case class SingleDatasetMetricChecks(
    describedDataset: DescribedDataset,
    checks: Seq[SingleMetricBasedCheck[_]] = Seq.empty
) {
  def withChecks(checks: Seq[SingleMetricBasedCheck[_]]) = this.copy(checks = this.checks ++ checks)
}

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
  * List of DualMetricBasedChecks to be run on a pair of datasets
  * @param describedDatasetA - the first dataset that is part of the comparison
  * @param describedDatasetB - the second dataset that is part of the comparison
  * @param checks - the checks to be done on the datasets
  */
case class DualDatasetMetricChecks(
    describedDatasetA: DescribedDataset,
    describedDatasetB: DescribedDataset,
    checks: Seq[DualMetricBasedCheck[_]]
)

/**
  * Main entry point which contains the suite of checks you want to perform
  * @param checkSuiteDescription - dsecription of the check suite
  * @param tags - any tags associated with the check suite
  * @param seqSingleDatasetMetricsChecks - list of metric based checks to perform on single datasets
  * @param seqDualDatasetMetricChecks - list of metric based checks where the metrics are compared across pairs of datasets
  * @param singleDatasetChecks - arbitrary checks performed on single datasets
  * @param datasetComparisonChecks - arbitrary checks performed on pairs of datasets
  * @param arbitraryChecks - any other arbitrary checks
  * @param deequChecks - checks to perform using deequ as the underlying checking mechanism
  * @param metricsPersister - how to persist metrics
  * @param deequMetricsRepository - how to persist deequ's metrics
  * @param checkResultCombiner - how the overall result status should be calculated
  */
case class ChecksSuite(
    checkSuiteDescription: String,
    tags: Map[String, String] = Map.empty,
    seqSingleDatasetMetricsChecks: Seq[SingleDatasetMetricChecks] = Seq.empty,
    seqDualDatasetMetricChecks: Seq[DualDatasetMetricChecks] = Seq.empty,
    singleDatasetChecks: Seq[SingleDatasetCheckWithDs] = Seq.empty,
    datasetComparisonChecks: Seq[DatasetComparisonCheckWithDs] = Seq.empty,
    arbitraryChecks: Seq[ArbitraryCheck] = Seq.empty,
    deequChecks: Seq[DeequCheck] = Seq.empty,
    metricsPersister: MetricsPersister = NullMetricsPersister,
    deequMetricsRepository: DeequMetricsRepository = new DeequNullMetricsRepository,
    qcResultsRepository: QcResultsRepository = new NullQcResultsRepository,
    checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
) extends ChecksSuiteBase {

  /**
    * Run all checks in the ChecksSuite
    *
    * @param timestamp - time the checks are being run
    * @param ec        - execution context
    * @return
    */
  override def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult] = {
    val metricBasedCheckResultsFut: Future[Seq[CheckResult]] = runMetricBasedChecks(timestamp)
    val singleDatasetCheckResults: Seq[CheckResult] = for {
      singleDatasetCheck <- singleDatasetChecks
      check <- singleDatasetCheck.checks
      checkResults = check.applyCheck(singleDatasetCheck.dataset)
    } yield checkResults
    val datasetComparisonCheckResults: Seq[CheckResult] = for {
      datasetComparisonCheck <- datasetComparisonChecks
      check <- datasetComparisonCheck.checks
      checkResults = check.applyCheck(datasetComparisonCheck.datasets)
    } yield checkResults
    val arbitraryCheckResults = arbitraryChecks.map(_.applyCheck)
    val deequCheckResults = getDeequCheckResults(deequChecks, timestamp, tags)

    for {
      metricBasedCheckResults <- metricBasedCheckResultsFut
      allCheckResults =
        metricBasedCheckResults ++ singleDatasetCheckResults ++ datasetComparisonCheckResults ++
          arbitraryCheckResults ++ deequCheckResults
      checkSuiteResult = ChecksSuiteResult(
        overallStatus = checkResultCombiner(allCheckResults),
        checkSuiteDescription = checkSuiteDescription,
        resultDescription = ChecksSuiteBase.getOverallCheckResultDescription(allCheckResults),
        checkResults = allCheckResults,
        timestamp = timestamp,
        tags
      )
      _ <- qcResultsRepository.save(checkSuiteResult)
    } yield {
      checkSuiteResult
    }
  }

  private def getDeequCheckResults(
      deequChecks: Seq[DeequCheck],
      timestamp: Instant,
      tags: Map[String, String]
  ): Seq[CheckResult] = {
    for {
      deequCheck <- deequChecks
      ds = deequCheck.describedDataset.ds
      verificationSuite: VerificationRunBuilder = VerificationSuite()
        .onData(ds.toDF)
        .useRepository(deequMetricsRepository)
        .saveOrAppendResult(ResultKey(timestamp.toEpochMilli))
      checkResult <-
        verificationSuite
          .addChecks(deequCheck.checks.map(_.check))
          .run()
          .toCheckResults(tags)
    } yield checkResult
  }

  /**
    * Calculates the minimum required metrics to calculate this check suite
    */
  private def getMinimumRequiredMetrics(
      seqSingleDatasetMetricsChecks: Seq[SingleDatasetMetricChecks],
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
      (singleDatasetMetricDescriptors |+| dualDatasetAMetricDescriptors |+| dualDatasetBMetricDescriptors)
        .mapValues(_.distinct)

    allMetricDescriptors
  }

  private def runMetricBasedChecks(
      timestamp: Instant
  )(implicit ec: ExecutionContext): Future[Seq[CheckResult]] = {
    val allMetricDescriptors: Map[DescribedDataset, List[MetricDescriptor]] =
      getMinimumRequiredMetrics(seqSingleDatasetMetricsChecks, seqDualDatasetMetricChecks)
    val calculatedMetrics: Map[DescribedDataset, Map[MetricDescriptor, MetricValue]] =
      allMetricDescriptors.map {
        case (describedDataset, metricDescriptors) =>
          val metricValues: Map[MetricDescriptor, MetricValue] =
            MetricsCalculator.calculateMetrics(describedDataset.ds, metricDescriptors)
          (describedDataset, metricValues)
      }

    val metricsToSave = calculatedMetrics.map {
      case (describedDataset, metrics) =>
        (
          describedDataset.description,
          metrics.map {
            case (descriptor, value) => (descriptor.toSimpleMetricDescriptor, value)
          }
        )
    }
    val storedMetricsFut = metricsPersister.save(timestamp, metricsToSave)

    for {
      _ <- storedMetricsFut
    } yield {
      val singleDatasetCheckResults: Seq[CheckResult] = seqSingleDatasetMetricsChecks.flatMap { singleDatasetMetricChecks =>
        val checks = singleDatasetMetricChecks.checks
        val datasetDescription = singleDatasetMetricChecks.describedDataset.description
        val metricsForDs: Map[MetricDescriptor, MetricValue] =
          calculatedMetrics(singleDatasetMetricChecks.describedDataset)
        val checkResults: Seq[CheckResult] = checks.map(
          _.applyCheckOnMetrics(metricsForDs).withDatasourceDescription(datasetDescription)
        )
        checkResults
      }

      val dualDatasetCheckResults: Seq[CheckResult] = seqDualDatasetMetricChecks.flatMap { dualDatasetMetricChecks =>
        val checks = dualDatasetMetricChecks.checks
        val describedDatasetA = dualDatasetMetricChecks.describedDatasetA
        val describedDatasetB = dualDatasetMetricChecks.describedDatasetB
        val metricsForDsA: Map[MetricDescriptor, MetricValue] =
          calculatedMetrics(describedDatasetA)
        val metricsForDsB: Map[MetricDescriptor, MetricValue] =
          calculatedMetrics(describedDatasetB)
        val checkResults: Seq[CheckResult] = checks.map(
          _.applyCheckOnMetrics(metricsForDsA, metricsForDsB)
            .withDatasourceDescription(
              s"${describedDatasetA.description} compared to ${describedDatasetB.description}"
            )
        )
        checkResults
      }

      singleDatasetCheckResults ++ dualDatasetCheckResults
    }

  }
}
