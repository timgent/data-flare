package com.github.timgent.sparkdataquality

import java.time.Instant

import com.github.timgent.sparkdataquality.checkssuite.{ChecksSuite, ChecksSuiteResult, ChecksSuitesResults}
import com.github.timgent.sparkdataquality.repository.QcResultsRepository
import cats.implicits._

import scala.concurrent.{ExecutionContext, Future}

object QualityChecker {
  def doQualityChecks(qualityChecks: List[ChecksSuite],
                      qcResultsRepository: QcResultsRepository,
                      timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuitesResults] = {
    val qualityCheckResultsFut: Future[List[ChecksSuiteResult]] = qualityChecks.toList.traverse(_.run(timestamp))
    for {
      qualityCheckResults <- qualityCheckResultsFut
      _ <- qcResultsRepository.save(qualityCheckResults)
    } yield ChecksSuitesResults(qualityCheckResults)
  }
}
