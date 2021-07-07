package com.github.timgent.dataflare.checkssuite

import java.time.Instant
import cats.implicits._
import com.github.timgent.dataflare.FlareError.MetricCalculationError
import com.github.timgent.dataflare.checks.ArbDualDsCheck.DatasetPair
import com.github.timgent.dataflare.checks.DatasourceDescription.{DualDsDescription, SingleDsDescription}
import com.github.timgent.dataflare.checks.QCCheck.{DualDsQCCheck, SingleDsCheck}
import com.github.timgent.dataflare.checks._
import com.github.timgent.dataflare.checks.metrics.{DualMetricCheck, SingleMetricCheck}
import com.github.timgent.dataflare.metrics.{MetricDescriptor, MetricValue, MetricsCalculator}
import com.github.timgent.dataflare.repository.QcResultsRepoErr.QcResultsRepoException
import com.github.timgent.dataflare.repository.{MetricsPersister, NullMetricsPersister, NullQcResultsRepository, QcResultsRepository}
import org.apache.spark.sql.Dataset

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * A dataset with description
  * @param ds - the dataset
  * @param description - description of the dataset
  */
case class DescribedDs(ds: Dataset[_], description: String) {
  def datasourceDescription: SingleDsDescription = SingleDsDescription(description)
}

/**
  * A pair of [[DescribedDs]]s
  *
  * @param ds - the first described dataset
  * @param dsToCompare - the second described dataset
  */
case class DescribedDsPair(ds: DescribedDs, dsToCompare: DescribedDs) {
  def datasourceDescription: DualDsDescription = DualDsDescription(ds.description, dsToCompare.description)

  private[dataflare] def rawDatasetPair = DatasetPair(ds.ds, dsToCompare.ds)
}

trait ChecksSuiteErr {
  def throwErr: Nothing
}

/**
  * Main entry point which contains the suite of checks you want to perform
  * @param checkSuiteDescription - description of the check suite
  * @param tags - any tags associated with the check suite
  * @param singleDsChecks - map from a single dataset to a list of checks on that dataset
  * @param dualDsChecks - map from a pair of datasets to a list of checks to do on that pair of datasets
  * @param arbitraryChecks - any other arbitrary checks
  * @param metricsToTrack - metrics to track (even if no checks on them)
  * @param metricsPersister - how to persist metrics
  * @param checkResultCombiner - how the overall result status should be calculated
  */
case class ChecksSuite(
    checkSuiteDescription: String,
    tags: Map[String, String] = Map.empty,
    singleDsChecks: Map[DescribedDs, Seq[SingleDsCheck]] = Map.empty,
    dualDsChecks: Map[DescribedDsPair, Seq[DualDsQCCheck]] = Map.empty,
    arbitraryChecks: Seq[ArbitraryCheck] = Seq.empty,
    metricsToTrack: Map[DescribedDs, Seq[MetricDescriptor]] = Map.empty,
    metricsPersister: MetricsPersister = NullMetricsPersister,
    qcResultsRepository: QcResultsRepository = new NullQcResultsRepository,
    checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
) {

  private val arbSingleDsChecks: Map[DescribedDs, Seq[ArbSingleDsCheck]] = singleDsChecks.map {
    case (dds, checks) =>
      val relevantChecks = checks.collect { case check: ArbSingleDsCheck => check }
      (dds, relevantChecks)
  }

  private val singleMetricChecks: Map[DescribedDs, Seq[SingleMetricCheck[_]]] = singleDsChecks.map {
    case (dds, checks) =>
      val relevantChecks = checks.collect { case check: SingleMetricCheck[_] => check }
      (dds, relevantChecks)
  }

  private val arbDualDsChecks: Map[DescribedDsPair, Seq[ArbDualDsCheck]] = dualDsChecks.map {
    case (ddsPair, checks) =>
      val relevantChecks = checks.collect { case check: ArbDualDsCheck => check }
      (ddsPair, relevantChecks)
  }

  private val dualMetricChecks: Map[DescribedDsPair, Seq[DualMetricCheck[_]]] = dualDsChecks.map {
    case (ddsPair, checks) =>
      val relevantChecks = checks.collect { case check: DualMetricCheck[_] => check }
      (ddsPair, relevantChecks)
  }

  /**
    * Run all checks in the ChecksSuite and waits for computations to finish before returning (blocking the thread)
    *
    * @param timestamp - time the checks are being run
    * @param ec        - execution context
    * @return
    */
  @deprecated("Will be replaced by runBlockingV2 which surfaces errors", "July 2021")
  def runBlocking(timestamp: Instant, timeout: Duration = 1 minute)(implicit ec: ExecutionContext): ChecksSuiteResult =
    Await.result(run(timestamp), timeout)

  /**
    * Run all checks in the ChecksSuite and waits for computations to finish before returning (blocking the thread)
    *
    * @param timestamp - time the checks are being run
    * @param ec        - execution context
    * @return either an error or the ChecksSuiteResult
    */
  def runBlockingV2(timestamp: Instant, timeout: Duration = 1 minute)(implicit
      ec: ExecutionContext
  ): Either[ChecksSuiteErr, ChecksSuiteResult] =
    Await.result(runV2(timestamp), timeout)

  /**
    * Run all checks in the ChecksSuite asynchronously, returning a Future
    *
    * @param timestamp - time the checks are being run
    * @param ec        - execution context
    * @return
    */
  @deprecated("Will be replaced by runV2 which surfaces errors in the return type", "July 2021")
  def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult] = {
    runV2(timestamp).map {
      case Left(err)               => err.throwErr
      case Right(checkSuiteResult) => checkSuiteResult
    }
  }

  /**
    * Run all checks in the ChecksSuite asynchronously, returning a Future with either an
    * error of the ChecksSuiteResult
    *
    * @param timestamp - time the checks are being run
    * @param ec        - execution context
    * @return
    */
  def runV2(timestamp: Instant)(implicit ec: ExecutionContext): Future[Either[ChecksSuiteErr, ChecksSuiteResult]] = {
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

    for {
      metricBasedCheckResults <- metricBasedCheckResultsFut
      allCheckResults =
        metricBasedCheckResults ++ singleDatasetCheckResults ++ datasetComparisonCheckResults ++
          arbitraryCheckResults
      checkSuiteResult = ChecksSuiteResult(
        overallStatus = checkResultCombiner(allCheckResults),
        checkSuiteDescription = checkSuiteDescription,
        checkResults = allCheckResults,
        timestamp = timestamp,
        tags
      )
      maybeSavedCheckSuiteResult <- qcResultsRepository.saveV2(checkSuiteResult)
    } yield {
      maybeSavedCheckSuiteResult
    }
  }

  /**
    * Calculates the minimum required metrics to calculate this check suite
    */
  private def getMinimumRequiredMetrics(
      seqSingleDatasetMetricsChecks: Map[DescribedDs, Seq[SingleMetricCheck[_]]],
      seqDualDatasetMetricChecks: Map[DescribedDsPair, Seq[DualMetricCheck[_]]],
      trackMetrics: Map[DescribedDs, Seq[MetricDescriptor]]
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
      (singleDatasetMetricDescriptors |+| dualDatasetAMetricDescriptors |+| dualDatasetBMetricDescriptors
        |+| trackMetrics.mapValues(_.toList))
        .mapValues(_.distinct)

    allMetricDescriptors
  }

  private def runMetricBasedChecks(
      timestamp: Instant
  )(implicit ec: ExecutionContext): Future[Seq[CheckResult]] = {
    val allMetricDescriptors: Map[DescribedDs, List[MetricDescriptor]] =
      getMinimumRequiredMetrics(singleMetricChecks, dualMetricChecks, metricsToTrack)
    val calculatedMetrics: Map[DescribedDs, Either[MetricCalculationError, Map[MetricDescriptor, MetricValue]]] =
      allMetricDescriptors.map {
        case (describedDataset, metricDescriptors) =>
          val metricValues: Either[MetricCalculationError, Map[MetricDescriptor, MetricValue]] =
            MetricsCalculator.calculateMetrics(describedDataset, metricDescriptors)
          (describedDataset, metricValues)
      }

    val metricsToSave = calculatedMetrics.collect {
      case (describedDataset, Right(metrics)) =>
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
      val singleDatasetCheckResults: Seq[CheckResult] = singleMetricChecks.toSeq.flatMap {
        case (dds, checks) =>
          val datasetDescription = SingleDsDescription(dds.description)
          val maybeMetricsForDs: Either[MetricCalculationError, Map[MetricDescriptor, MetricValue]] = calculatedMetrics(dds)
          val checkResults: Seq[CheckResult] = checks.map { check =>
            maybeMetricsForDs match {
              case Left(err) => check.getMetricErrorCheckResult(dds.datasourceDescription, err)
              case Right(metricsForDs: Map[MetricDescriptor, MetricValue]) =>
                check.applyCheckOnMetrics(metricsForDs).withDatasourceDescription(datasetDescription)
            }
          }
          checkResults
      }

      val dualDatasetCheckResults: Seq[CheckResult] = dualMetricChecks.toSeq.flatMap {
        case (ddsPair, checks) =>
          val dds = ddsPair.ds
          val ddsToCompare = ddsPair.dsToCompare
          val maybeMetricsForDsA: Either[MetricCalculationError, Map[MetricDescriptor, MetricValue]] = calculatedMetrics(dds)
          val maybeMetricsForDsB: Either[MetricCalculationError, Map[MetricDescriptor, MetricValue]] = calculatedMetrics(ddsToCompare)
          val datasourceDescription = DualDsDescription(dds.description, ddsToCompare.description)
          val checkResults: Seq[CheckResult] = checks.map { check =>
            (maybeMetricsForDsA, maybeMetricsForDsB) match {
              case (Right(metricsForDsA), Right(metricsForDsB)) =>
                check
                  .applyCheckOnMetrics(metricsForDsA, metricsForDsB, datasourceDescription)
                  .withDatasourceDescription(datasourceDescription)
              case (Left(dsErr), Left(dsToCompareErr)) =>
                check.getMetricErrorCheckResult(ddsPair.datasourceDescription, dsErr, dsToCompareErr)
              case (_, Left(dsToCompareErr)) => check.getMetricErrorCheckResult(ddsPair.datasourceDescription, dsToCompareErr)
              case (Left(dsErr), _)          => check.getMetricErrorCheckResult(ddsPair.datasourceDescription, dsErr)
            }
          }
          checkResults
      }

      singleDatasetCheckResults ++ dualDatasetCheckResults
    }

  }
}
