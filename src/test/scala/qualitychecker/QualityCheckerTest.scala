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
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import qualitychecker.CheckResultDetails.NoDetails
import qualitychecker.QualityChecks.{ArbitraryQualityChecks, DatasetComparisonQualityChecks, DeequQualityChecks, SingleDatasetQualityChecks}
import qualitychecker.constraint.QCConstraint.DatasetComparisonConstraint.DatasetPair
import qualitychecker.constraint.QCConstraint.{ArbitraryConstraint, DatasetComparisonConstraint, DeequQCConstraint, SingleDatasetConstraint}
import qualitychecker.constraint.{ConstraintStatus, RawConstraintResult}
import utils.TestDataClass

import scala.util.Success

class QualityCheckerTest extends AnyWordSpec with DatasetSuiteBase with Matchers {

  import spark.implicits._

  val now: Instant = Instant.now

  "doQualityChecks" should { // TODO: Add in assertions on the details of results for each constraint
    "be able to do deequ quality checks and store check results and underlying metrics in a repository" in {
      val testDataset = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDF
      val qcResultsRepository = new InMemoryQcResultsRepository
      val deequMetricsRepository: Option[InMemoryMetricsRepository] = Some(new InMemoryMetricsRepository)

      val qualityChecks = List(
        DeequQualityChecks(testDataset, "sample deequ checks", Seq(DeequQCConstraint(
          Check(CheckLevel.Error, "size check").hasSize(_ == 3)
        )))(deequMetricsRepository)
      )

      val qcResults: Seq[QualityCheckResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[QualityCheckResult[NoDetails.type]] = qcResultsRepository.loadAll
      val persistedDeequMetrics: Seq[AnalysisResult] = deequMetricsRepository.get.load().get()

      qcResults.size shouldBe 1
      qcResults.head.checkType shouldBe QcType.DeequQualityCheck
      qcResults.head.timestamp shouldBe now
      qcResults.head.checkDescription shouldBe "sample deequ checks"
      qcResults.head.overallStatus shouldBe CheckStatus.Success

      persistedQcResults.size shouldBe 1
      persistedQcResults.head.checkType shouldBe QcType.DeequQualityCheck
      persistedQcResults.head.timestamp shouldBe now
      persistedQcResults.head.checkDescription shouldBe "sample deequ checks"
      persistedQcResults.head.overallStatus shouldBe CheckStatus.Success

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

      val constraints = Seq(SingleDatasetConstraint("sumNumberCheck") {
        dataset =>
          val sumOfNumbers = dataset.agg(sum("number")).as[Long].collect.head
          if (sumOfNumbers > 2)
            RawConstraintResult(ConstraintStatus.Success, "sumNumberCheck was successful!")
          else
            RawConstraintResult(ConstraintStatus.Error, "sumNumberCheck was not successful :(")
      })

      val qualityChecks = List(SingleDatasetQualityChecks(testDataset, checkDescription, constraints))

      val qcResults: Seq[QualityCheckResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[QualityCheckResult[NoDetails]] = qcResultsRepository.loadAll

      qcResults.size shouldBe 1
      qcResults.head.checkType shouldBe QcType.SingleDatasetQualityCheck
      qcResults.head.timestamp shouldBe now
      qcResults.head.checkDescription shouldBe checkDescription
      qcResults.head.overallStatus shouldBe CheckStatus.Success

      persistedQcResults.size shouldBe 1
      persistedQcResults.head.checkType shouldBe QcType.SingleDatasetQualityCheck
      persistedQcResults.head.timestamp shouldBe now
      persistedQcResults.head.checkDescription shouldBe checkDescription
      persistedQcResults.head.overallStatus shouldBe CheckStatus.Success
    }

    "be able to run custom 2 table checks and store results in a repository" in {
      val testDataset = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS
      val datasetToCompare = List((1, "a"), (2, "b"), (3, "c"), (4, "d")).map(TestDataClass.tupled).toDS
      val qcResultsRepository = new InMemoryQcResultsRepository

      val constraint = DatasetComparisonConstraint("Table counts equal"){case DatasetPair(ds, dsToCompare) =>
        val countsAreEqual = ds.count == dsToCompare.count
        val constraintStatus = if (countsAreEqual) ConstraintStatus.Success else ConstraintStatus.Error
        val resultDescription = if (countsAreEqual) "counts were equal" else "counts were not equal"
        RawConstraintResult(constraintStatus, resultDescription)
      }
      val qualityChecks = Seq(DatasetComparisonQualityChecks(testDataset, datasetToCompare, "table A vs table B comparison", Seq(constraint)))

      val qcResults: Seq[QualityCheckResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[QualityCheckResult[NoDetails.type]] = qcResultsRepository.loadAll

      qcResults.size shouldBe 1
      qcResults.head.checkType shouldBe QcType.DatasetComparisonQualityCheck
      qcResults.head.timestamp shouldBe now
      qcResults.head.checkDescription shouldBe "table A vs table B comparison"
      qcResults.head.overallStatus shouldBe CheckStatus.Error

      persistedQcResults.size shouldBe 1
      persistedQcResults.head.checkType shouldBe QcType.DatasetComparisonQualityCheck
      persistedQcResults.head.timestamp shouldBe now
      persistedQcResults.head.checkDescription shouldBe "table A vs table B comparison"
      persistedQcResults.head.overallStatus shouldBe CheckStatus.Error
    }

    "be able to run completely arbitrary checks and store results in a repository" in {
      val dataset1 = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS
      val dataset2 = List((1, "a"), (2, "b"), (3, "c"), (4, "d")).map(TestDataClass.tupled).toDS
      val dataset3 = List((1, "a"), (2, "b"), (3, "c"), (4, "d")).map(TestDataClass.tupled).toDS
      val qcResultsRepository = new InMemoryQcResultsRepository

      val constraint = ArbitraryConstraint("datasets contain same letters"){
        val letterCountPostJoin = dataset1.join(dataset2, Seq("letter")).join(dataset3, Seq("letter")).select("letter").count
        val maxLetterCountPreJoin = List(dataset1.count, dataset2.count, dataset3.count).max
        if (letterCountPostJoin < maxLetterCountPreJoin)
          RawConstraintResult(ConstraintStatus.Error, "Letters across the datasets were not the same")
        else
          RawConstraintResult(ConstraintStatus.Success, "Datasets all contained the same letters")

      }
      val qualityChecks = Seq(ArbitraryQualityChecks("table A, table B, and table C comparison", Seq(constraint)))

      val qcResults: Seq[QualityCheckResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
      val persistedQcResults: Seq[QualityCheckResult[NoDetails.type]] = qcResultsRepository.loadAll

      qcResults.size shouldBe 1
      qcResults.head.checkType shouldBe QcType.ArbitraryQualityCheck
      qcResults.head.timestamp shouldBe now
      qcResults.head.checkDescription shouldBe "table A, table B, and table C comparison"
      qcResults.head.overallStatus shouldBe CheckStatus.Error

      persistedQcResults.size shouldBe 1
      persistedQcResults.head.checkType shouldBe QcType.ArbitraryQualityCheck
      persistedQcResults.head.timestamp shouldBe now
      persistedQcResults.head.checkDescription shouldBe "table A, table B, and table C comparison"
      persistedQcResults.head.overallStatus shouldBe CheckStatus.Error
    }
  }
}

