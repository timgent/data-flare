package com.github.timgent.sparkdataquality

import java.time.Instant

import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.github.timgent.sparkdataquality.checks.DatasetComparisonCheck.DatasetPair
import com.github.timgent.sparkdataquality.checks.{ArbitraryCheck, CheckResult, CheckStatus, DatasetComparisonCheck, DeequQCCheck, QcType, RawCheckResult, SingleDatasetCheck}
import com.github.timgent.sparkdataquality.checkssuite._
import com.github.timgent.sparkdataquality.repository.InMemoryQcResultsRepository
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import com.github.timgent.sparkdataquality.utils.TestDataClass
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.Success

class QualityCheckerTest extends AsyncWordSpec with DatasetSuiteBase with Matchers {

  import spark.implicits._

  val now: Instant = Instant.now

  def checkResultAndPersistedResult(qcResult: ChecksSuiteResult, persistedQcResult: ChecksSuiteResult)(
    timestamp: Instant,
    checkSuiteDescription: String,
    checkStatus: CheckSuiteStatus,
    resultDescription: String,
    checkResults: Seq[CheckResult],
    checkTags: Map[String, String]
  ): Assertion = {
    qcResult.timestamp shouldBe timestamp
    qcResult.checkSuiteDescription shouldBe checkSuiteDescription
    qcResult.overallStatus shouldBe checkStatus
    qcResult.resultDescription shouldBe resultDescription
    qcResult.checkResults shouldBe checkResults
    qcResult.checkTags shouldBe checkTags
    persistedQcResult.timestamp shouldBe timestamp
    persistedQcResult.checkSuiteDescription shouldBe checkSuiteDescription
    persistedQcResult.overallStatus shouldBe checkStatus
    persistedQcResult.resultDescription shouldBe resultDescription
    persistedQcResult.checkResults shouldBe checkResults
    persistedQcResult.checkTags shouldBe checkTags
  }

  "doQualityChecks" should {

    "be able to do deequ quality checks and store check results and underlying metrics in a repository" in {
      val testDataset = DescribedDataset(List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDF, "testDataset")
      val qcResultsRepository = new InMemoryQcResultsRepository
      val deequMetricsRepository: InMemoryMetricsRepository = new InMemoryMetricsRepository

      val deequQcConstraint = DeequQCCheck(Check(CheckLevel.Error, "size check").hasSize(_ == 3))
      val qualityChecks = List(
        ChecksSuite(
          checkSuiteDescription = "sample deequ checks",
          deequChecks = Seq(DeequCheck(testDataset, Seq(deequQcConstraint))),
          tags = someTags,
          deequMetricsRepository = deequMetricsRepository)
      )

      for {
        qcResults <- QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now).map(_.results)
        persistedQcResults <- qcResultsRepository.loadAll
        persistedDeequMetrics = deequMetricsRepository.load().get()
      } yield {
        qcResults.size shouldBe 1
        persistedQcResults.size shouldBe 1
        checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
          timestamp = now,
          checkSuiteDescription = "sample deequ checks",
          checkStatus = CheckSuiteStatus.Success,
          resultDescription = "All Deequ checks were successful",
          checkResults = Seq(CheckResult(QcType.DeequQualityCheck, CheckStatus.Success, "Deequ check was successful", deequQcConstraint.description)),
          checkTags = someTags
        )

        persistedDeequMetrics.size shouldBe 1
        persistedDeequMetrics.head.resultKey shouldBe ResultKey(now.toEpochMilli, Map.empty)
        persistedDeequMetrics.head.analyzerContext shouldBe AnalyzerContext(Map(
          Size(None) -> DoubleMetric(Entity.Dataset, "Size", "*", Success(3.0))
        ))
      }
    }

    "be able to run custom single table checks and store results in a repository" in {
      val testDataset = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS
      val qcResultsRepository = new InMemoryQcResultsRepository
      val checkDescription = "DB: X, table: Y"

      val singleDatasetCheck = SingleDatasetCheck("someSingleDatasetCheck") {
        dataset => RawCheckResult(CheckStatus.Error, "someSingleDatasetCheck was not successful")
      }
      val checks = Seq(singleDatasetCheck)

      val qualityChecks = List(SingleDatasetChecksSuite(DescribedDataset(testDataset, datasourceDescription), checkDescription, checks, someTags))

      for {
        qcResults: Seq[ChecksSuiteResult] <- QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now).map(_.results)
        persistedQcResults: Seq[ChecksSuiteResult] <- qcResultsRepository.loadAll
      } yield {
        checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
          timestamp = now,
          checkSuiteDescription = "DB: X, table: Y",
          checkStatus = CheckSuiteStatus.Error,
          resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
          checkResults = Seq(CheckResult(QcType.SingleDatasetQualityCheck, CheckStatus.Error,
            "someSingleDatasetCheck was not successful", singleDatasetCheck.description, Some(datasourceDescription))),
          checkTags = someTags
        )
      }
    }

    "be able to run custom 2 table checks and store results in a repository" in {
      val testDataset = DescribedDataset(List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS, "testDataset")
      val datasetToCompare = DescribedDataset(List((1, "a"), (2, "b"), (3, "c"), (4, "d")).map(TestDataClass.tupled).toDS, "datasetToCompare")
      val datasetPair = DescribedDatasetPair(testDataset, datasetToCompare)
      val qcResultsRepository = new InMemoryQcResultsRepository

      val datasetComparisonCheck = DatasetComparisonCheck("Table counts equal") { case DatasetPair(ds, dsToCompare) =>
        RawCheckResult(CheckStatus.Error, "counts were not equal")
      }
      val qualityChecks = List(ChecksSuite("table A vs table B comparison",
        datasetComparisonChecks = Seq(DatasetComparisonCheckWithDs(datasetPair, Seq(datasetComparisonCheck))),
        tags = someTags)
      )

      for {
        qcResults: Seq[ChecksSuiteResult] <- QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now).map(_.results)
        persistedQcResults: Seq[ChecksSuiteResult] <- qcResultsRepository.loadAll
      } yield {
        checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
          timestamp = now,
          checkSuiteDescription = "table A vs table B comparison",
          checkStatus = CheckSuiteStatus.Error,
          resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
          checkResults = Seq(CheckResult(QcType.DatasetComparisonQualityCheck, CheckStatus.Error, "counts were not equal", datasetComparisonCheck.description)),
          checkTags = someTags
        )
      }
    }

    "be able to run completely arbitrary checks and store results in a repository" in {
      val qcResultsRepository = new InMemoryQcResultsRepository

      val arbitraryCheck = ArbitraryCheck("some arbitrary check") {
        RawCheckResult(CheckStatus.Error, "The arbitrary check failed!")
      }
      val qualityChecks = List(ChecksSuite("table A, table B, and table C comparison",
        arbitraryChecks = Seq(arbitraryCheck), tags = someTags))

      for {
        qcResults: Seq[ChecksSuiteResult] <- QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now).map(_.results)
        persistedQcResults: Seq[ChecksSuiteResult] <- qcResultsRepository.loadAll
      } yield {
        qcResults.size shouldBe 1
        qcResults.head.timestamp shouldBe now
        qcResults.head.checkSuiteDescription shouldBe "table A, table B, and table C comparison"
        qcResults.head.overallStatus shouldBe CheckSuiteStatus.Error

        persistedQcResults.size shouldBe 1
        persistedQcResults.head.timestamp shouldBe now
        persistedQcResults.head.checkSuiteDescription shouldBe "table A, table B, and table C comparison"
        persistedQcResults.head.overallStatus shouldBe CheckSuiteStatus.Error

        checkResultAndPersistedResult(qcResults.head, persistedQcResults.head)(
          timestamp = now,
          checkSuiteDescription = "table A, table B, and table C comparison",
          checkStatus = CheckSuiteStatus.Error,
          resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
          checkResults = Seq(CheckResult(QcType.ArbitraryQualityCheck, CheckStatus.Error, "The arbitrary check failed!", arbitraryCheck.description)),
          checkTags = someTags
        )
      }
    }
  }
}

