package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import cats.implicits._
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import com.github.timgent.sparkdataquality.checks.ArbDualDsCheck.DatasetPair
import com.github.timgent.sparkdataquality.checks.DatasourceDescription.{DualDsDescription, SingleDsDescription}
import com.github.timgent.sparkdataquality.checks.QCCheck.{DualDsQCCheck, SingleDsCheck}
import com.github.timgent.sparkdataquality.checks._
import com.github.timgent.sparkdataquality.checks.metrics.{DualMetricCheck, SingleMetricCheck}
import com.github.timgent.sparkdataquality.deequ.DeequHelpers.VerificationResultExtension
import com.github.timgent.sparkdataquality.deequ.DeequNullMetricsRepository
import com.github.timgent.sparkdataquality.metrics.{MetricDescriptor, MetricValue, MetricsCalculator}
import com.github.timgent.sparkdataquality.repository.{MetricsPersister, NullMetricsPersister, NullQcResultsRepository, QcResultsRepository}
import com.github.timgent.sparkdataquality.sparkdataquality.DeequMetricsRepository
import org.apache.spark.sql.Dataset

import scala.concurrent.{ExecutionContext, Future}

/**
  * A dataset with description
  * @param ds - the dataset
  * @param description - description of the dataset
  */
case class DescribedDs(ds: Dataset[_], description: String)

/**
  * A pair of [[DescribedDs]]s
 *
  * @param ds - the first described dataset
  * @param dsToCompare - the second described dataset
  */
case class DescribedDsPair(ds: DescribedDs, dsToCompare: DescribedDs) {
  def datasourceDescription: Option[DualDsDescription] = Some(DualDsDescription(ds.description, dsToCompare.description))

  private[sparkdataquality] def rawDatasetPair = DatasetPair(ds.ds, dsToCompare.ds)
}

/**
  * Main entry point which contains the suite of checks you want to perform
  * @param checkSuiteDescription - description of the check suite
  * @param tags - any tags associated with the check suite
  * @param singleDsChecks - map from a single dataset to a list of checks on that dataset
  * @param dualDsChecks - map from a pair of datasets to a list of checks to do on that pair of datasets
  * @param arbitraryChecks - any other arbitrary checks
  * @param metricsPersister - how to persist metrics
  * @param deequMetricsRepository - how to persist deequ's metrics
  * @param checkResultCombiner - how the overall result status should be calculated
  */
case class ChecksSuite(
                        checkSuiteDescription: String,
                        tags: Map[String, String] = Map.empty,
                        singleDsChecks: Map[DescribedDs, Seq[SingleDsCheck]] = Map.empty,
                        dualDsChecks: Map[DescribedDsPair, Seq[DualDsQCCheck]] = Map.empty,
                        arbitraryChecks: Seq[ArbitraryCheck] = Seq.empty,
                        metricsPersister: MetricsPersister = NullMetricsPersister,
                        deequMetricsRepository: DeequMetricsRepository = new DeequNullMetricsRepository,
                        qcResultsRepository: QcResultsRepository = new NullQcResultsRepository,
                        checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
) extends ChecksSuiteBase {

  private val arbSingleDsChecks: Map[DescribedDs, Seq[ArbSingleDsCheck]] = singleDsChecks.map { case (dds, checks) =>
    val relevantChecks = checks.collect { case check: ArbSingleDsCheck => check }
    (dds, relevantChecks)
  }

  private val singleMetricChecks: Map[DescribedDs, Seq[SingleMetricCheck[_]]]  = singleDsChecks.map { case (dds, checks) =>
    val relevantChecks = checks.collect { case check: SingleMetricCheck[_] => check }
    (dds, relevantChecks)
  }

  private val deequChecks: Map[DescribedDs, Seq[DeequQCCheck]] = singleDsChecks.map { case (dds, checks) =>
    val relevantChecks = checks.collect { case check: DeequQCCheck => check }
    (dds, relevantChecks)
  }

  private val arbDualDsChecks: Map[DescribedDsPair, Seq[ArbDualDsCheck]] = dualDsChecks.map { case (ddsPair, checks) =>
    val relevantChecks = checks.collect { case check: ArbDualDsCheck => check}
    (ddsPair, relevantChecks)
  }

  private val dualMetricChecks: Map[DescribedDsPair, Seq[DualMetricCheck[_]]] = dualDsChecks.map { case (ddsPair, checks) =>
    val relevantChecks = checks.collect { case check: DualMetricCheck[_] => check}
    (ddsPair, relevantChecks)
  }

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
      (dds, checks) <- arbSingleDsChecks.toSeq
      check <- checks
      checkResults = check.applyCheck(dds)
    } yield checkResults
    val datasetComparisonCheckResults: Seq[CheckResult] = for {
      (ddsPair, checks) <- arbDualDsChecks.toSeq
      check <- checks
      checkResults = check.applyCheck(ddsPair)
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
                                    deequChecks: Map[DescribedDs, Seq[DeequQCCheck]],
                                    timestamp: Instant,
                                    tags: Map[String, String]
  ): Seq[CheckResult] = {
    for {
      (dds, checks) <- deequChecks.toSeq
      verificationSuite: VerificationRunBuilder = VerificationSuite()
        .onData(dds.ds.toDF)
        .useRepository(deequMetricsRepository)
        .saveOrAppendResult(ResultKey(timestamp.toEpochMilli))
      checkResult <-
        verificationSuite
          .addChecks(checks.map(_.check))
          .run()
          .toCheckResults(tags)
    } yield checkResult
  }

  /**
    * Calculates the minimum required metrics to calculate this check suite
    */
  private def getMinimumRequiredMetrics(
                                         seqSingleDatasetMetricsChecks: Map[DescribedDs, Seq[SingleMetricCheck[_]]],
                                         seqDualDatasetMetricChecks: Map[DescribedDsPair, Seq[DualMetricCheck[_]]]
  ): Map[DescribedDs, List[MetricDescriptor]] = {
    val singleDatasetMetricDescriptors: Map[DescribedDs, List[MetricDescriptor]] = (for {
      (dds, checks) <- seqSingleDatasetMetricsChecks
      metricDescriptors = checks.map(_.metric).toList
    } yield (dds, metricDescriptors)).groupBy(_._1).mapValues(_.flatMap(_._2).toList)

    val dualDatasetAMetricDescriptors: Map[DescribedDs, List[MetricDescriptor]] = (for {
      (ddsPair, checks) <- seqDualDatasetMetricChecks
      describedDatasetA: DescribedDs = ddsPair.ds
      metricDescriptors = checks.map(_.dsMetric).toList
    } yield (describedDatasetA, metricDescriptors)).groupBy(_._1).mapValues(_.flatMap(_._2).toList)

    val dualDatasetBMetricDescriptors: Map[DescribedDs, List[MetricDescriptor]] = (for {
      (ddsPair, checks) <- seqDualDatasetMetricChecks
      describedDatasetB: DescribedDs = ddsPair.dsToCompare
      metricDescriptors = checks.map(_.dsToCompareMetric).toList
    } yield (describedDatasetB, metricDescriptors)).groupBy(_._1).mapValues(_.flatMap(_._2).toList)

    val allMetricDescriptors: Map[DescribedDs, List[MetricDescriptor]] =
      (singleDatasetMetricDescriptors |+| dualDatasetAMetricDescriptors |+| dualDatasetBMetricDescriptors)
        .mapValues(_.distinct)

    allMetricDescriptors
  }

  private def runMetricBasedChecks(
      timestamp: Instant
  )(implicit ec: ExecutionContext): Future[Seq[CheckResult]] = {
    val allMetricDescriptors: Map[DescribedDs, List[MetricDescriptor]] =
      getMinimumRequiredMetrics(singleMetricChecks, dualMetricChecks)
    val calculatedMetrics: Map[DescribedDs, Map[MetricDescriptor, MetricValue]] =
      allMetricDescriptors.map {
        case (describedDataset, metricDescriptors) =>
          val metricValues: Map[MetricDescriptor, MetricValue] =
            MetricsCalculator.calculateMetrics(describedDataset.ds, metricDescriptors)
          (describedDataset, metricValues)
      }

    val metricsToSave = calculatedMetrics.map {
      case (describedDataset, metrics) =>
        (
          SingleDsDescription(describedDataset.description),
          metrics.map {
            case (descriptor, value) => (descriptor.toSimpleMetricDescriptor, value)
          }
        )
    }
    val storedMetricsFut = metricsPersister.save(timestamp, metricsToSave)

    for {
      _ <- storedMetricsFut
    } yield {
      val singleDatasetCheckResults: Seq[CheckResult] = singleMetricChecks.toSeq.flatMap { case (dds, checks) =>
        val datasetDescription = SingleDsDescription(dds.description)
        val metricsForDs: Map[MetricDescriptor, MetricValue] =
          calculatedMetrics(dds)
        val checkResults: Seq[CheckResult] = checks.map(
          _.applyCheckOnMetrics(metricsForDs).withDatasourceDescription(datasetDescription)
        )
        checkResults
      }

      val dualDatasetCheckResults: Seq[CheckResult] = dualMetricChecks.toSeq.flatMap { case (ddsPair, checks) =>
        val dds = ddsPair.ds
        val ddsToCompare = ddsPair.dsToCompare
        val metricsForDsA: Map[MetricDescriptor, MetricValue] = calculatedMetrics(dds)
        val metricsForDsB: Map[MetricDescriptor, MetricValue] = calculatedMetrics(ddsToCompare)
        val datasourceDescription = DualDsDescription(dds.description, ddsToCompare.description)
        val checkResults: Seq[CheckResult] = checks.map(
          _.applyCheckOnMetrics(metricsForDsA, metricsForDsB, datasourceDescription)
            .withDatasourceDescription(datasourceDescription)
        )
        checkResults
      }

      singleDatasetCheckResults ++ dualDatasetCheckResults
    }

  }
}
