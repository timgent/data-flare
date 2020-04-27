package qualitychecker

import java.time.Instant

import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.{AnalysisResult, ResultKey}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.functions.sum
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import qualitychecker.CheckResultDetails.NoDetails
import qualitychecker.ChecksSuite.{ArbitraryChecksSuite, DatasetComparisonChecksSuite, DeequChecksSuite, SingleDatasetChecksSuite}
import qualitychecker.checks.QCCheck.DatasetComparisonCheck.DatasetPair
import qualitychecker.checks.QCCheck.{ArbitraryCheck, DatasetComparisonCheck, DeequQCCheck, SingleDatasetCheck}
import qualitychecker.checks.{CheckResult, CheckStatus, RawCheckResult}
import utils.TestDataClass

import scala.util.Success

class QualityCheckerTest extends AnyWordSpec with DatasetSuiteBase with Matchers {

  import spark.implicits._

  val now: Instant = Instant.now

  def checkResultAndPersistedResult(qcResult: ChecksSuiteResult[_], persistedQcResult: ChecksSuiteResult[NoDetails])(
    checkType: QcType.Value,
    timestamp: Instant,
    checkSuiteDescription: String,
    checkStatus: CheckSuiteStatus.Value,
    resultDescription: String,
    checkResults: Seq[CheckResult]
  ): Unit = {
    qcResult.checkType shouldBe checkType
    qcResult.timestamp shouldBe timestamp
    qcResult.checkSuiteDescription shouldBe checkSuiteDescription
    qcResult.overallStatus shouldBe checkStatus
    qcResult.resultDescription shouldBe resultDescription
    qcResult.checkResults shouldBe checkResults
    persistedQcResult.checkType shouldBe checkType
    persistedQcResult.timestamp shouldBe timestamp
    persistedQcResult.checkSuiteDescription shouldBe checkSuiteDescription
    persistedQcResult.overallStatus shouldBe checkStatus
    persistedQcResult.resultDescription shouldBe resultDescription
    persistedQcResult.checkResults shouldBe checkResults
  }

  "doQualityChecks" should {

    "be able to do deequ quality checks and store check results and underlying metrics in a repository" in {
      val testDataset = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDF
      val qcResultsRepository = new InMemoryQcResultsRepository
      val deequMetricsRepository: Option[InMemoryMetricsRepository] = Some(new InMemoryMetricsRepository)

      val deequQcConstraint = DeequQCCheck(Check(CheckLevel.Error, "size check").hasSize(_ == 3))
      val qualityChecks = List(
        DeequChecksSuite(testDataset, "sample deequ checks", Seq(deequQcConstraint))(deequMetricsRepository)
      )

      val qcResults: Seq[ChecksSuiteResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[ChecksSuiteResult[NoDetails]] = qcResultsRepository.loadAll
      val persistedDeequMetrics: Seq[AnalysisResult] = deequMetricsRepository.get.load().get()

      qcResults.size shouldBe 1
      persistedQcResults.size shouldBe 1
      checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
        checkType = QcType.DeequQualityCheck,
        timestamp = now,
        checkSuiteDescription = "sample deequ checks",
        checkStatus = CheckSuiteStatus.Success,
        resultDescription = "All Deequ checks were successful",
        checkResults = Seq(CheckResult(CheckStatus.Success, "Deequ check was successful", deequQcConstraint))
      )

      persistedDeequMetrics.size shouldBe 1
      persistedDeequMetrics.head.resultKey shouldBe ResultKey(now.toEpochMilli, Map.empty)
      persistedDeequMetrics.head.analyzerContext shouldBe AnalyzerContext(Map(
        Size(None) -> DoubleMetric(Entity.Dataset, "Size", "*", Success(3.0))
      ))

    }

    "be able to run custom single table checks and store results in a repository" in {
      val testDataset = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS
      val qcResultsRepository = new InMemoryQcResultsRepository
      val checkDescription = "DB: X, table: Y"

      val singleDatasetCheck = SingleDatasetCheck("someSingleDatasetCheck") {
        dataset => RawCheckResult(CheckStatus.Error, "someSingleDatasetCheck was not successful")
      }
      val checks = Seq(singleDatasetCheck)

      val qualityChecks = List(SingleDatasetChecksSuite(testDataset, checkDescription, checks))

      val qcResults: Seq[ChecksSuiteResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[ChecksSuiteResult[NoDetails]] = qcResultsRepository.loadAll

      checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
        checkType = QcType.SingleDatasetQualityCheck,
        timestamp = now,
        checkSuiteDescription = "DB: X, table: Y",
        checkStatus = CheckSuiteStatus.Error,
        resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
        checkResults = Seq(CheckResult(CheckStatus.Error, "someSingleDatasetCheck was not successful", singleDatasetCheck))
      )
    }

    "be able to run custom 2 table checks and store results in a repository" in {
      val testDataset = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS
      val datasetToCompare = List((1, "a"), (2, "b"), (3, "c"), (4, "d")).map(TestDataClass.tupled).toDS
      val qcResultsRepository = new InMemoryQcResultsRepository

      val datasetComparisonCheck = DatasetComparisonCheck("Table counts equal") { case DatasetPair(ds, dsToCompare) =>
        RawCheckResult(CheckStatus.Error, "counts were not equal")
      }
      val qualityChecks = Seq(DatasetComparisonChecksSuite(testDataset, datasetToCompare, "table A vs table B comparison", Seq(datasetComparisonCheck)))

      val qcResults: Seq[ChecksSuiteResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[ChecksSuiteResult[NoDetails]] = qcResultsRepository.loadAll

      checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
        checkType = QcType.DatasetComparisonQualityCheck,
        timestamp = now,
        checkSuiteDescription = "table A vs table B comparison",
        checkStatus = CheckSuiteStatus.Error,
        resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
        checkResults = Seq(CheckResult(CheckStatus.Error, "counts were not equal", datasetComparisonCheck))
      )
    }

    "be able to run completely arbitrary checks and store results in a repository" in {
      val qcResultsRepository = new InMemoryQcResultsRepository

      val arbitraryCheck = ArbitraryCheck("some arbitrary check") {
          RawCheckResult(CheckStatus.Error, "The arbitrary check failed!")
      }
      val qualityChecks = Seq(ArbitraryChecksSuite("table A, table B, and table C comparison", Seq(arbitraryCheck)))

      val qcResults: Seq[ChecksSuiteResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[ChecksSuiteResult[NoDetails]] = qcResultsRepository.loadAll

      qcResults.size shouldBe 1
      qcResults.head.checkType shouldBe QcType.ArbitraryQualityCheck
      qcResults.head.timestamp shouldBe now
      qcResults.head.checkSuiteDescription shouldBe "table A, table B, and table C comparison"
      qcResults.head.overallStatus shouldBe CheckSuiteStatus.Error

      persistedQcResults.size shouldBe 1
      persistedQcResults.head.checkType shouldBe QcType.ArbitraryQualityCheck
      persistedQcResults.head.timestamp shouldBe now
      persistedQcResults.head.checkSuiteDescription shouldBe "table A, table B, and table C comparison"
      persistedQcResults.head.overallStatus shouldBe CheckSuiteStatus.Error

      checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
        checkType = QcType.ArbitraryQualityCheck,
        timestamp = now,
        checkSuiteDescription = "table A, table B, and table C comparison",
        checkStatus = CheckSuiteStatus.Error,
        resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
        checkResults = Seq(CheckResult(CheckStatus.Error, "The arbitrary check failed!", arbitraryCheck))
      )
    }
  }
}

