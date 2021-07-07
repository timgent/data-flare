package com.github.timgent.dataflare.repository

import com.github.timgent.dataflare.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.dataflare.checks.QcType.{ArbDualDsCheck, ArbSingleDsCheck}
import com.github.timgent.dataflare.checks._
import com.github.timgent.dataflare.checkssuite.CheckSuiteStatus.{Error, Success}
import com.github.timgent.dataflare.checkssuite.{CheckSuiteStatus, ChecksSuiteResult}
import com.github.timgent.dataflare.utils.CommonFixtures._
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import sttp.client3._

import scala.concurrent.Future
import scala.concurrent.duration._

class DfApiQcResultsRepositoryTest
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with EsTestUtils
    with ScalaCheckDrivenPropertyChecks {

  "DfApiResultsRepository.save" should {
    def generateRawCheckResult(qcType: QcType, suffix: String, status: CheckStatus) =
      CheckResult(qcType, status, s"checkResult$suffix", SimpleCheckDescription(s"checkDescription$suffix"))

    val uri = uri"http://127.0.0.1:8080"
    implicit val patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 1 second)

    "post check suite results to the API" in {
      val repo: DfApiQcResultsRepository =
        new DfApiQcResultsRepository(uri)

      val checkResultA1 =
        generateRawCheckResult(ArbSingleDsCheck, "A1", CheckStatus.Success)
      val checkResultA2 =
        generateRawCheckResult(ArbSingleDsCheck, "A2", CheckStatus.Success)
      val checkResultB1 =
        generateRawCheckResult(ArbDualDsCheck, "B1", CheckStatus.Error)
      val checkResultB2 =
        generateRawCheckResult(ArbDualDsCheck, "B2", CheckStatus.Error)
      val checkResultB1Success =
        generateRawCheckResult(ArbDualDsCheck, "B1", CheckStatus.Error)
      val checkResultB2Success =
        generateRawCheckResult(ArbDualDsCheck, "B2", CheckStatus.Error)
      val initialResultsToInsert: List[ChecksSuiteResult] = List(
        ChecksSuiteResult(
          Success,
          "checkSuiteA",
          Seq(checkResultA1, checkResultA2),
          now,
          someTags
        ),
        ChecksSuiteResult(
          Error,
          "checkSuiteB",
          Seq(checkResultB1, checkResultB2),
          now,
          someTags
        )
      )
      val moreResultsToInsert: List[ChecksSuiteResult] = List(
        ChecksSuiteResult(
          CheckSuiteStatus.Success,
          "checkSuiteB",
          Seq(checkResultB1Success, checkResultB2Success),
          now.plusSeconds(10),
          someTags
        )
      )

      def storedResultsFut(): Future[List[ChecksSuiteResult]] = repo.loadAll.map(_.right.get)

      for {
        _ <- repo.save(initialResultsToInsert)
        _ <- checkStoredResultsAre(storedResultsFut, initialResultsToInsert)
        _ <- repo.save(moreResultsToInsert)
        finalAssertion <- checkStoredResultsAre(storedResultsFut, initialResultsToInsert ++ moreResultsToInsert)
      } yield {
        finalAssertion
      }
    }
  }
}
