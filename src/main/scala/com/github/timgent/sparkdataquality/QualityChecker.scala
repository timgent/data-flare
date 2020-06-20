package com.github.timgent.sparkdataquality

import java.time.Instant

import com.github.timgent.sparkdataquality.checkssuite.{
  ChecksSuiteBase,
  ChecksSuiteResult,
  ChecksSuitesResults
}
import com.github.timgent.sparkdataquality.repository.QcResultsRepository
import cats.implicits._

import scala.concurrent.{ExecutionContext, Future}

object QualityChecker {

  /**
    *
    * @param qualityChecks List of quality check suites that checks should be run for
    * @param qcResultsRepository Repository for storing the results of the quality checks
    * @param timestamp The timestamp the checks are being run for, used when persisting checks or metrics
    * @param ec The execution context
    * @return A Future of check suite results
    */
  def doQualityChecks(
      qualityChecks: List[ChecksSuiteBase],
      qcResultsRepository: QcResultsRepository,
      timestamp: Instant
  )(implicit ec: ExecutionContext): Future[ChecksSuitesResults] = {
    val qualityCheckResultsFut: Future[List[ChecksSuiteResult]] =
      qualityChecks.traverse(_.run(timestamp))
    for {
      qualityCheckResults <- qualityCheckResultsFut
      _ <- qcResultsRepository.save(qualityCheckResults)
    } yield ChecksSuitesResults(qualityCheckResults)
  }

  /**
    *
    * @param qualityChecks Quality check suite that checks should be run for
    * @param qcResultsRepository Repository for storing the results of the quality checks
    * @param timestamp The timestamp the checks are being run for, used when persisting checks or metrics
    * @param ec The execution context
    * @return A Future of check suite results
    */
  def doQualityChecks(
      qualityChecks: ChecksSuiteBase,
      qcResultsRepository: QcResultsRepository,
      timestamp: Instant
  )(implicit ec: ExecutionContext): Future[ChecksSuitesResults] = {
    doQualityChecks(List(qualityChecks), qcResultsRepository, timestamp)
  }
}
