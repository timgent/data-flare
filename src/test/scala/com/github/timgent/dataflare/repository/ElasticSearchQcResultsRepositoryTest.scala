package com.github.timgent.dataflare.repository

import java.time.Instant
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.arbInstantJdk8
import com.github.timgent.dataflare.checks.CheckDescription.{
  DualMetricCheckDescription,
  SimpleCheckDescription,
  SingleMetricCheckDescription
}
import com.github.timgent.dataflare.checks.DatasourceDescription.{DualDsDescription, OtherDsDescription, SingleDsDescription}
import com.github.timgent.dataflare.checks.QcType.{ArbDualDsCheck, ArbSingleDsCheck}
import com.github.timgent.dataflare.checks.{CheckDescription, CheckResult, CheckStatus, DatasourceDescription, QcType}
import com.github.timgent.dataflare.checkssuite.CheckSuiteStatus.{Error, Success}
import com.github.timgent.dataflare.checkssuite.{CheckSuiteStatus, ChecksSuiteResult}
import com.github.timgent.dataflare.generators.Generators.arbSimpleMetricDescriptor
import com.github.timgent.dataflare.json.CustomEncodings.{checksSuiteResultDecoder, checksSuiteResultEncoder, datasourceDescriptionEncoder}
import com.github.timgent.dataflare.utils.CommonFixtures._
import com.sksamuel.elastic4s.testkit.DockerTests
import io.circe.parser._
import io.circe.syntax._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.Future
import scala.concurrent.duration._

class ElasticSearchQcResultsRepositoryTest
    extends AsyncWordSpec
    with Matchers
    with DockerTests
    with Eventually
    with EsTestUtils
    with ScalaCheckDrivenPropertyChecks {

  implicit val checkSuiteStatusArb = Arbitrary(Gen.oneOf(CheckSuiteStatus.values))
  implicit val checkStatusArb = Arbitrary(Gen.oneOf(CheckStatus.values))
  implicit val simpleCheckDescriptionArb = Arbitrary(Gen.resultOf(SimpleCheckDescription))
  implicit val dualMetricCheckDescriptionArb = Arbitrary(Gen.resultOf(DualMetricCheckDescription))
  implicit val singleMetricCheckDescriptionArb = Arbitrary(Gen.resultOf(SingleMetricCheckDescription))
  implicit val checkDescriptionArb: Arbitrary[CheckDescription] = Arbitrary(
    Gen.oneOf(arbitrary[SimpleCheckDescription], arbitrary[DualMetricCheckDescription], arbitrary[SingleMetricCheckDescription])
  )
  implicit val qcTypeArb = Arbitrary(Gen.oneOf(QcType.values))
  implicit val singleDsDescriptionArb = Arbitrary(Gen.resultOf(SingleDsDescription))
  implicit val dualDsDescriptionArb = Arbitrary(Gen.resultOf(DualDsDescription))
  implicit val otherDsDescriptionArb = Arbitrary(Gen.resultOf(OtherDsDescription))
  implicit val datasourceDescriptionArb: Arbitrary[Option[DatasourceDescription]] =
    Arbitrary(Gen.option(Gen.oneOf(arbitrary[SingleDsDescription], arbitrary[DualDsDescription], arbitrary[OtherDsDescription])))
  implicit val checkResultArb = Arbitrary(for {
    qcType <- arbitrary[QcType]
    status <- arbitrary[CheckStatus]
    resultDescription <- arbitrary[String]
    checkDescription <- arbitrary[CheckDescription]
    datasourceDescription <- arbitrary[Option[DatasourceDescription]]
  } yield CheckResult(qcType, status, resultDescription, checkDescription, datasourceDescription))
  val checksSuiteResultGen: Gen[ChecksSuiteResult] = for {
    overallStatus <- arbitrary[CheckSuiteStatus]
    checkSuiteDescription <- arbitrary[String]
    checkResults <- arbitrary[Seq[CheckResult]]
    timestamp <- arbitrary[Instant]
    checkTags <- arbitrary[Map[String, String]]
  } yield ChecksSuiteResult(overallStatus, checkSuiteDescription, checkResults, timestamp, checkTags)
  implicit val checksSuiteResultArb: Arbitrary[ChecksSuiteResult] = Arbitrary(checksSuiteResultGen)

  "ElasticSearchQcResultsRepository.save" should {
    def generateRawCheckResult(qcType: QcType, suffix: String, status: CheckStatus) =
      CheckResult(qcType, status, s"checkResult$suffix", SimpleCheckDescription(s"checkDescription$suffix"))

    val someIndex = "index_name"
    implicit val patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 1 second)

    cleanIndex(someIndex)
    "Append check suite results to the index" in {
      val repo: ElasticSearchQcResultsRepository =
        new ElasticSearchQcResultsRepository(client, someIndex)

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

      def storedResultsFut(): Future[List[ChecksSuiteResult]] = repo.loadAll

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

  "ElasticSearchQcResultsRepository.checksSuiteResultEncoder" should {
    "encode a ChecksSuiteResult in JSON as expected" in {
      val json = ChecksSuiteResult(
        CheckSuiteStatus.Success,
        "someCheckSuiteDescription",
        Seq(
          CheckResult(
            QcType.SingleMetricCheck,
            CheckStatus.Success,
            "someResultDescriptionA",
            SimpleCheckDescription("someCheckDescriptionA"),
            Some(SingleDsDescription("someDatasourceDescription"))
          ),
          CheckResult(
            QcType.ArbSingleDsCheck,
            CheckStatus.Error,
            "someResultDescriptionB",
            SimpleCheckDescription("someCheckDescriptionB"),
            None
          )
        ),
        now,
        Map("someTagKey" -> "someTagValue")
      ).asJson
      val expectedJson = parse(
        s"""
           |{
           |  "overallStatus" : "Success",
           |  "checkSuiteDescription" : "someCheckSuiteDescription",
           |  "checkResults" : [
           |    {
           |      "qcType" : "SingleMetricCheck",
           |      "status" : "Success",
           |      "resultDescription" : "someResultDescriptionA",
           |      "checkDescription" : {
           |        "desc" : "someCheckDescriptionA",
           |        "type" : "SimpleCheckDescription"
           |      },
           |      "datasourceDescription" : {
           |        "type": "SingleDs",
           |        "datasource": "someDatasourceDescription"
           |      }
           |    },
           |    {
           |      "qcType" : "ArbSingleDsCheck",
           |      "status" : "Error",
           |      "resultDescription" : "someResultDescriptionB",
           |      "checkDescription" : {
           |        "desc" : "someCheckDescriptionB",
           |        "type" : "SimpleCheckDescription"
           |      },
           |      "datasourceDescription" : null
           |    }
           |  ],
           |  "timestamp" : "${now.toString}",
           |  "checkTags" : { "someTagKey": "someTagValue" }
           |}
           |""".stripMargin
      ).right.get
      json shouldBe expectedJson
    }

    "always be possible to encode and decode a ChecksSuiteResult from JSON" in {
      import ElasticSearchQcResultsRepository._
      forAll { checksSuiteResult: ChecksSuiteResult =>
        val afterRoundTrip = checksSuiteResult.asJson.as[ChecksSuiteResult]
        afterRoundTrip.right.get shouldBe checksSuiteResult
      }
    }
  }

  "ElasticSearchQcResultsRepository.datasourceDescriptionEncoder" should {
    "encoder SingleDsDescription correctly" in {
      val description: DatasourceDescription = SingleDsDescription("myDatasource")
      val json = description.asJson
      val expectedJson =
        parse("""{
          |"type": "SingleDs",
          |"datasource": "myDatasource"
          |}""".stripMargin).right.get
      json shouldBe expectedJson
    }
    "encoder DualDsDescription correctly" in {
      val description: DatasourceDescription = DualDsDescription("myDatasourceA", "myDatasourceB")
      val json = description.asJson
      val expectedJson =
        parse("""{
                |"type": "DualDs",
                |"datasourceA": "myDatasourceA",
                |"datasourceB": "myDatasourceB"
                |}""".stripMargin).right.get
      json shouldBe expectedJson
    }
    "encoder OtherDsDescription correctly" in {
      val description: DatasourceDescription = OtherDsDescription("myDatasource")
      val json = description.asJson
      val expectedJson =
        parse("""{
                |"type": "OtherDs",
                |"datasource": "myDatasource"
                |}""".stripMargin).right.get
      json shouldBe expectedJson
    }
  }
}
