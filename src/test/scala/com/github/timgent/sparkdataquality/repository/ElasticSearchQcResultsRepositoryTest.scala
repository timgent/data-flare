package com.github.timgent.sparkdataquality.repository

import com.github.timgent.sparkdataquality.checks.QcType.{
  DatasetComparisonQualityCheck,
  SingleDatasetQualityCheck
}
import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.sparkdataquality.checkssuite.CheckSuiteStatus.{Error, Success}
import com.github.timgent.sparkdataquality.checkssuite.{CheckSuiteStatus, ChecksSuiteResult}
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import com.sksamuel.elastic4s.testkit.DockerTests
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration._

class ElasticSearchQcResultsRepositoryTest
    extends AsyncWordSpec
    with Matchers
    with DockerTests
    with Eventually
    with EsTestUtils {
  "ElasticSearchQcResultsRepository.save" should {
    def generateRawCheckResult(qcType: QcType, suffix: String, status: CheckStatus) =
      CheckResult(qcType, status, s"checkResult$suffix", s"checkDescription$suffix")

    val someIndex = "index_name"
    implicit val patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 1 second)

    cleanIndex(someIndex)
    "Append check suite results to the index" in {
      val repo: ElasticSearchQcResultsRepository =
        new ElasticSearchQcResultsRepository(client, someIndex)

      val checkResultA1 =
        generateRawCheckResult(SingleDatasetQualityCheck, "A1", CheckStatus.Success)
      val checkResultA2 =
        generateRawCheckResult(SingleDatasetQualityCheck, "A2", CheckStatus.Success)
      val checkResultB1 =
        generateRawCheckResult(DatasetComparisonQualityCheck, "B1", CheckStatus.Error)
      val checkResultB2 =
        generateRawCheckResult(DatasetComparisonQualityCheck, "B2", CheckStatus.Error)
      val checkResultB1Success =
        generateRawCheckResult(DatasetComparisonQualityCheck, "B1", CheckStatus.Error)
      val checkResultB2Success =
        generateRawCheckResult(DatasetComparisonQualityCheck, "B2", CheckStatus.Error)
      val initialResultsToInsert: List[ChecksSuiteResult] = List(
        ChecksSuiteResult(
          Success,
          "checkSuiteA",
          "resultA",
          Seq(checkResultA1, checkResultA2),
          now,
          someTags
        ),
        ChecksSuiteResult(
          Error,
          "checkSuiteB",
          "resultB",
          Seq(checkResultB1, checkResultB2),
          now,
          someTags
        )
      )
      val moreResultsToInsert: List[ChecksSuiteResult] = List(
        ChecksSuiteResult(
          CheckSuiteStatus.Success,
          "checkSuiteB",
          "resultB",
          Seq(checkResultB1Success, checkResultB2Success),
          now.plusSeconds(10),
          someTags
        )
      )

      def storedResultsFut(): Future[List[ChecksSuiteResult]] = repo.loadAll

      for {
        _ <- repo.save(initialResultsToInsert)
        _ <- checkStoredResultsAre(storedResultsFut, initialResultsToInsert)
        _ <- repo.save(moreResultsToInsert)
        finalAssertion <-
          checkStoredResultsAre(storedResultsFut, initialResultsToInsert ++ moreResultsToInsert)
      } yield {
        finalAssertion
      }
    }
  }
}
