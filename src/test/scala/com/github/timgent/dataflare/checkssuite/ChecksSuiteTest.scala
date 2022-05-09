package com.github.timgent.dataflare.checkssuite

import java.time.Instant

import cats.implicits._
import com.github.timgent.dataflare.FlareError
import com.github.timgent.dataflare.FlareError.MetricCalculationError
import com.github.timgent.dataflare.checks.ArbDualDsCheck.DatasetPair
import com.github.timgent.dataflare.checks.CheckDescription.{DualMetricCheckDescription, SingleMetricCheckDescription}
import com.github.timgent.dataflare.checks.DatasourceDescription.{DualDsDescription, SingleDsDescription}
import com.github.timgent.dataflare.checks.metrics.{DualMetricCheck, SingleMetricCheck}
import com.github.timgent.dataflare.checks._
import com.github.timgent.dataflare.metrics.MetricDescriptor.{SizeMetric, SumValuesMetric}
import com.github.timgent.dataflare.metrics.MetricValue.LongMetric
import com.github.timgent.dataflare.metrics.{MetricComparator, MetricDescriptor, MetricFilter, SimpleMetricDescriptor}
import com.github.timgent.dataflare.repository.{InMemoryMetricsPersister, InMemoryQcResultsRepository}
import com.github.timgent.dataflare.thresholds.AbsoluteThreshold
import com.github.timgent.dataflare.utils.CommonFixtures._
import com.github.timgent.dataflare.utils.TestDataClass
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.Success

class ChecksSuiteTest extends AsyncWordSpec with DatasetSuiteBase with Matchers {
  import spark.implicits._
  def checkResultAndPersistedResult(
      qcResult: ChecksSuiteResult,
      persistedQcResult: ChecksSuiteResult
  )(
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
    qcResult.checkResults shouldBe checkResults
    qcResult.checkTags shouldBe checkTags
    persistedQcResult.timestamp shouldBe timestamp
    persistedQcResult.checkSuiteDescription shouldBe checkSuiteDescription
    persistedQcResult.overallStatus shouldBe checkStatus
    persistedQcResult.checkResults shouldBe checkResults
    persistedQcResult.checkTags shouldBe checkTags
  }

  def assertCheckResultHasMetricErrorFields(
      checksSuiteResult: ChecksSuiteResult,
      checkDescription: CheckDescription,
      qcType: QcType,
      datasourceDescription: DatasourceDescription
  ) = {
    checksSuiteResult.checkResults.size shouldBe 1
    val checkResult = checksSuiteResult.checkResults.head
    checkResult.qcType shouldBe qcType
    checkResult.status shouldBe CheckStatus.Error
    checkResult.checkDescription shouldBe checkDescription
    checkResult.datasourceDescription shouldBe Some(datasourceDescription)
    qcType match {
      case QcType.SingleMetricCheck | QcType.DualMetricCheck =>
        checkResult.resultDescription shouldBe "Check failed due to issue calculating metrics for this dataset"
      case QcType.ArbSingleDsCheck | QcType.ArbDualDsCheck =>
        checkResult.resultDescription shouldBe "Check failed due to unexpected exception during evaluation"
    }
  }

  def assertMetricErrorsRelateToCorrectDs(error: FlareError, badCheckDs: DescribedDs) = {
    error.msg shouldBe s"""One of the metrics defined on dataset ${badCheckDs.description} could not be calculated
                          |Metrics used were:
                          |- metricName=SumValues, filterDescription=no filter, onColumn=nonexistent""".stripMargin
    error.datasourceDescription shouldBe Some(badCheckDs.datasourceDescription)
  }

  def assertArbCheckErrorsRelateToCorrectDs(error: FlareError, expectedDsDescription: DatasourceDescription) = {
    assert(error.msg.contains("This check failed due to an exception being thrown during evaluation"))
    error.err shouldBe a[Some[_]]
    error.datasourceDescription shouldBe Some(expectedDsDescription)
  }

  lazy val dsA = Seq(
    NumberString(1, "a"),
    NumberString(2, "b"),
    NumberString(3, "c")
  ).toDS
  lazy val dsB = Seq(
    NumberString(1, "a"),
    NumberString(2, "b"),
    NumberString(3, "c")
  ).toDS
  lazy val ddsA = DescribedDs(dsA, "dsA")
  lazy val ddsB = DescribedDs(dsB, "dsB")
  lazy val ddsPair = DescribedDsPair(ddsA, ddsB)
  val badMetric = SumValuesMetric[LongMetric]("nonexistent")
  val goodMetric = SizeMetric()

  import spark.implicits._

  "ChecksSuite" which {
    "has single metric based checks" should {
      "calculate metrics based checks on single datasets" in {
        val ds = Seq(
          NumberString(1, "a"),
          NumberString(2, "b")
        ).toDS
        val checks: Map[DescribedDs, Seq[SingleMetricCheck[LongMetric]]] = Map(
          DescribedDs(ds, datasourceDescription) ->
            Seq(
              SingleMetricCheck
                .sizeCheck(AbsoluteThreshold(Some(2L), Some(2L)), MetricFilter.noFilter)
            )
        )
        val checkSuiteDescription = "my first metricsCheckSuite"
        val metricsBasedChecksSuite =
          ChecksSuite(checkSuiteDescription, singleDsChecks = checks, tags = someTags)

        for {
          checkResults: ChecksSuiteResult <- metricsBasedChecksSuite.run(now)
        } yield {
          checkResults shouldBe ChecksSuiteResult(
            CheckSuiteStatus.Success,
            checkSuiteDescription,
            Seq(
              CheckResult(
                QcType.SingleMetricCheck,
                CheckStatus.Success,
                "Size of 2 was within the range between 2 and 2",
                SingleMetricCheckDescription("SizeCheck", SimpleMetricDescriptor("Size", Some("no filter"))),
                Some(SingleDsDescription(datasourceDescription))
              )
            ),
            now,
            someTags
          )
        }
      }

      "calculate single dataset metrics checks across a number of datasets" in {
        val dsA = Seq(
          NumberString(1, "a"),
          NumberString(2, "b")
        )
        val dsB = Seq(
          NumberString(1, "a"),
          NumberString(2, "b"),
          NumberString(3, "c")
        )
        val checks: Seq[SingleMetricCheck[_]] = Seq(
          SingleMetricCheck.sizeCheck(AbsoluteThreshold(Some(2L), Some(2L)), MetricFilter.noFilter)
        )
        val singleDatasetChecks = Map(
          DescribedDs(dsA.toDS, "dsA") -> checks,
          DescribedDs(dsB.toDS, "dsB") -> checks
        )
        val checkSuiteDescription = "my first metricsCheckSuite"
        val metricsBasedChecksSuite = ChecksSuite(
          checkSuiteDescription,
          singleDsChecks = singleDatasetChecks,
          tags = someTags
        )

        for {
          checkResults: ChecksSuiteResult <- metricsBasedChecksSuite.run(now)
        } yield {
          checkResults shouldBe ChecksSuiteResult(
            CheckSuiteStatus.Error,
            checkSuiteDescription,
            Seq(
              CheckResult(
                QcType.SingleMetricCheck,
                CheckStatus.Success,
                "Size of 2 was within the range between 2 and 2",
                SingleMetricCheckDescription("SizeCheck", SimpleMetricDescriptor("Size", Some("no filter"))),
                Some(SingleDsDescription("dsA"))
              ),
              CheckResult(
                QcType.SingleMetricCheck,
                CheckStatus.Error,
                "Size of 3 was outside the range between 2 and 2",
                SingleMetricCheckDescription("SizeCheck", SimpleMetricDescriptor("Size", Some("no filter"))),
                Some(SingleDsDescription("dsB"))
              )
            ),
            now,
            someTags
          )
        }
      }

      "return an error if the check is invalid" in {
        val badCheck = SingleMetricCheck(badMetric, "badCheck")(_ => RawCheckResult(CheckStatus.Success, "I'll fail"))
        val checksSuite = ChecksSuite("badSuite", singleDsChecks = Map(ddsA -> Seq(badCheck)))
        for {
          checksSuiteResult <- checksSuite.run(now)
        } yield {
          assertCheckResultHasMetricErrorFields(
            checksSuiteResult,
            badCheck.description,
            QcType.SingleMetricCheck,
            ddsA.datasourceDescription
          )
          checksSuiteResult.checkResults.head.errors.size shouldBe 1
          assertMetricErrorsRelateToCorrectDs(checksSuiteResult.checkResults.head.errors.head, ddsA)
        }
      }
    }
    "has dual metric based checks" should {

      "calculate metrics based checks between datasets" in {
        val simpleSizeMetric = MetricDescriptor.SizeMetric()
        val metricChecks = Seq(
          DualMetricCheck(simpleSizeMetric, simpleSizeMetric, "check size metrics are equal", MetricComparator.metricsAreEqual)
        )
        val dualDatasetChecks = Map(
          DescribedDsPair(DescribedDs(dsA, "dsA"), DescribedDs(dsB, "dsB")) ->
            metricChecks
        )
        val checkSuiteDescription = "my first metricsCheckSuite"
        val metricsBasedChecksSuite = ChecksSuite(
          checkSuiteDescription,
          dualDsChecks = dualDatasetChecks,
          tags = someTags
        )

        for {
          checkResults: ChecksSuiteResult <- metricsBasedChecksSuite.run(now)
        } yield {
          checkResults shouldBe ChecksSuiteResult(
            CheckSuiteStatus.Success,
            checkSuiteDescription,
            Seq(
              CheckResult(
                QcType.DualMetricCheck,
                CheckStatus.Success,
                "metric comparison passed. dsA with LongMetric(3) was compared to dsB with LongMetric(3)",
                DualMetricCheckDescription(
                  "check size metrics are equal",
                  SimpleMetricDescriptor("Size", Some("no filter"), None, None),
                  SimpleMetricDescriptor("Size", Some("no filter"), None, None),
                  "metrics are equal"
                ),
                Some(DualDsDescription("dsA", "dsB"))
              )
            ),
            now,
            someTags
          )
        }
      }

      "return an error if the first check is invalid" in {
        val badCheck = DualMetricCheck(badMetric, goodMetric, "someCheck", MetricComparator.metricsAreEqual)
        val checksSuite = ChecksSuite("desc", dualDsChecks = Map(ddsPair -> Seq(badCheck)))
        for {
          checksSuiteResult <- checksSuite.run(now)
        } yield {
          assertCheckResultHasMetricErrorFields(
            checksSuiteResult,
            badCheck.description,
            QcType.DualMetricCheck,
            ddsPair.datasourceDescription
          )
          checksSuiteResult.checkResults.head.errors.size shouldBe 1
          assertMetricErrorsRelateToCorrectDs(checksSuiteResult.checkResults.head.errors.head, ddsA)
        }
      }
      "return an error if the second check is invalid" in {
        val badCheck = DualMetricCheck(goodMetric, badMetric, "someCheck", MetricComparator.metricsAreEqual)
        val checksSuite = ChecksSuite("desc", dualDsChecks = Map(ddsPair -> Seq(badCheck)))
        for {
          checksSuiteResult <- checksSuite.run(now)
        } yield {
          assertCheckResultHasMetricErrorFields(
            checksSuiteResult,
            badCheck.description,
            QcType.DualMetricCheck,
            ddsPair.datasourceDescription
          )
          checksSuiteResult.checkResults.head.errors.size shouldBe 1
          assertMetricErrorsRelateToCorrectDs(checksSuiteResult.checkResults.head.errors.head, ddsB)
        }
      }

      "return an error if both checks are invalid" in {
        val badCheck = DualMetricCheck(badMetric, badMetric, "someCheck", MetricComparator.metricsAreEqual)
        val checksSuite = ChecksSuite("desc", dualDsChecks = Map(ddsPair -> Seq(badCheck)))
        for {
          checksSuiteResult <- checksSuite.run(now)
        } yield {
          assertCheckResultHasMetricErrorFields(
            checksSuiteResult,
            badCheck.description,
            QcType.DualMetricCheck,
            ddsPair.datasourceDescription
          )
          val errors = checksSuiteResult.checkResults.head.errors
          errors.size shouldBe 2
          val ddsAErr = errors.find(_.datasourceDescription.contains(ddsA.datasourceDescription)).get
          val ddsBErr = errors.find(_.datasourceDescription.contains(ddsB.datasourceDescription)).get
          assertMetricErrorsRelateToCorrectDs(ddsAErr, ddsA)
          assertMetricErrorsRelateToCorrectDs(ddsBErr, ddsB)
        }
      }

    }

    "has any metrics based checks at all" should {
      "store all metrics in a metrics repository" in {
        val simpleSizeMetric = MetricDescriptor.SizeMetric()
        val dsA = Seq(
          NumberString(1, "a"),
          NumberString(2, "b"),
          NumberString(3, "c")
        ).toDS
        val dsB = Seq(
          NumberString(1, "a"),
          NumberString(2, "b")
        ).toDS
        val dsC = Seq(
          NumberString(1, "a")
        ).toDS
        val dualMetricChecks = Seq(
          DualMetricCheck(simpleSizeMetric, simpleSizeMetric, "check size metrics are equal", MetricComparator.metricsAreEqual)
        )
        val dualDatasetChecks = Map(DescribedDsPair(DescribedDs(dsA, "dsA"), DescribedDs(dsB, "dsB")) -> dualMetricChecks)
        val singleDatasetChecks: Map[DescribedDs, Seq[SingleMetricCheck[LongMetric]]] = Map(
          DescribedDs(dsC, "dsC") ->
            Seq(
              SingleMetricCheck
                .sizeCheck(AbsoluteThreshold(Some(2L), Some(2L)), MetricFilter.noFilter)
            )
        )
        val checkSuiteDescription = "my first metricsCheckSuite"
        val inMemoryMetricsPersister = new InMemoryMetricsPersister
        val metricsBasedChecksSuite = ChecksSuite(
          checkSuiteDescription,
          singleDsChecks = singleDatasetChecks,
          tags = someTags,
          dualDsChecks = dualDatasetChecks,
          metricsPersister = inMemoryMetricsPersister
        )

        for {
          _ <- metricsBasedChecksSuite.run(now)
          storedMetrics <- inMemoryMetricsPersister.loadAll
        } yield storedMetrics shouldBe Map(
          now -> Map(
            SingleDsDescription("dsA") -> Map(
              simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(3L)
            ),
            SingleDsDescription("dsB") -> Map(
              simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(2L)
            ),
            SingleDsDescription("dsC") -> Map(
              simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(1L)
            )
          )
        )
      }
    }

    "specifies specific metrics to track" should {
      "store specified metrics in a metrics repository" in {
        val simpleSizeMetric = MetricDescriptor.SizeMetric()
        val simpleSumMetric = MetricDescriptor.SumValuesMetric[LongMetric]("number")
        val dsA = Seq(
          NumberString(1, "a"),
          NumberString(2, "b"),
          NumberString(3, "c")
        ).toDS
        val ddsA = DescribedDs(dsA, "dsA")
        val dsB = Seq(NumberString(1, "a")).toDS
        val ddsB = DescribedDs(dsB, "dsB")
        val checkSuiteDescription = "my first metricsCheckSuite"
        val inMemoryMetricsPersister = new InMemoryMetricsPersister
        val metricsBasedChecksSuite = ChecksSuite(
          checkSuiteDescription,
          metricsToTrack = Map(ddsA -> Seq(simpleSizeMetric, simpleSumMetric), ddsB -> Seq(simpleSizeMetric)),
          metricsPersister = inMemoryMetricsPersister
        )

        for {
          _ <- metricsBasedChecksSuite.run(now)
          storedMetrics <- inMemoryMetricsPersister.loadAll
        } yield storedMetrics shouldBe Map(
          now -> Map(
            SingleDsDescription("dsA") -> Map(
              simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(3L),
              simpleSumMetric.toSimpleMetricDescriptor -> LongMetric(6L)
            ),
            SingleDsDescription("dsB") -> Map(
              simpleSizeMetric.toSimpleMetricDescriptor -> LongMetric(1L)
            )
          )
        )
      }
    }

    "contains single dataset checks" should {
      "run custom single table checks and store results in a repository" in {
        val testDataset = List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS
        val qcResultsRepository = new InMemoryQcResultsRepository
        val checkDescription = "DB: X, table: Y"

        val singleDatasetCheck = ArbSingleDsCheck("someSingleDatasetCheck") { dataset =>
          RawCheckResult(CheckStatus.Error, "someSingleDatasetCheck was not successful")
        }
        val checks = Seq(singleDatasetCheck)

        val qualityChecks = ChecksSuite(
          checkDescription,
          singleDsChecks = Map(DescribedDs(testDataset, datasourceDescription) -> checks),
          tags = someTags,
          qcResultsRepository = qcResultsRepository
        )

        for {
          qcResults: ChecksSuiteResult <- qualityChecks.run(now)
          persistedQcResults <- qcResultsRepository.loadAll
        } yield {
          checkResultAndPersistedResult(qcResults, persistedQcResults.right.get.head)(
            timestamp = now,
            checkSuiteDescription = "DB: X, table: Y",
            checkStatus = CheckSuiteStatus.Error,
            resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
            checkResults = Seq(
              CheckResult(
                QcType.ArbSingleDsCheck,
                CheckStatus.Error,
                "someSingleDatasetCheck was not successful",
                singleDatasetCheck.description,
                Some(SingleDsDescription(datasourceDescription))
              )
            ),
            checkTags = someTags
          )
        }
      }

      "return an error if the check is invalid" in {
        val badCheck = ArbSingleDsCheck("badCheck")(ds => RawCheckResult(CheckStatus.Success, ds.select("nonexistent").collect.toString))
        val checksSuite = ChecksSuite("badSuite", singleDsChecks = Map(ddsA -> Seq(badCheck)))
        for {
          checksSuiteResult <- checksSuite.run(now)
        } yield {
          assertCheckResultHasMetricErrorFields(
            checksSuiteResult,
            badCheck.description,
            QcType.ArbSingleDsCheck,
            ddsA.datasourceDescription
          )
          checksSuiteResult.checkResults.head.errors.size shouldBe 1
          assertArbCheckErrorsRelateToCorrectDs(checksSuiteResult.checkResults.head.errors.head, ddsA.datasourceDescription)
        }
      }
    }

    "contains dual dataset checks" should {
      "run custom 2 table checks and store results in a repository" in {
        val testDataset = DescribedDs(
          List((1, "a"), (2, "b"), (3, "c")).map(TestDataClass.tupled).toDS,
          "testDataset"
        )
        val datasetToCompare = DescribedDs(
          List((1, "a"), (2, "b"), (3, "c"), (4, "d")).map(TestDataClass.tupled).toDS,
          "datasetToCompare"
        )
        val datasetPair = DescribedDsPair(testDataset, datasetToCompare)
        val qcResultsRepository = new InMemoryQcResultsRepository

        val datasetComparisonCheck = ArbDualDsCheck("Table counts equal") {
          case DatasetPair(ds, dsToCompare) =>
            RawCheckResult(CheckStatus.Error, "counts were not equal")
        }
        val qualityChecks = ChecksSuite(
          "table A vs table B comparison",
          dualDsChecks = Map(datasetPair -> Seq(datasetComparisonCheck)),
          tags = someTags,
          qcResultsRepository = qcResultsRepository
        )

        for {
          qcResults: ChecksSuiteResult <- qualityChecks.run(now)
          persistedQcResults <- qcResultsRepository.loadAll
        } yield {
          checkResultAndPersistedResult(qcResults, persistedQcResults.right.get.head)(
            timestamp = now,
            checkSuiteDescription = "table A vs table B comparison",
            checkStatus = CheckSuiteStatus.Error,
            resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
            checkResults = Seq(
              CheckResult(
                QcType.ArbDualDsCheck,
                CheckStatus.Error,
                "counts were not equal",
                datasetComparisonCheck.description,
                Some(DualDsDescription("testDataset", "datasetToCompare"))
              )
            ),
            checkTags = someTags
          )
        }
      }

      "return an error if the check is invalid" in {
        val badCheck =
          ArbDualDsCheck("badCheck")(dsPair => RawCheckResult(CheckStatus.Success, dsPair.ds.select("nonexistent").collect.toString))
        val checksSuite = ChecksSuite("badSuite", dualDsChecks = Map(ddsPair -> Seq(badCheck)))
        for {
          checksSuiteResult <- checksSuite.run(now)
        } yield {
          assertCheckResultHasMetricErrorFields(
            checksSuiteResult,
            badCheck.description,
            QcType.ArbDualDsCheck,
            ddsPair.datasourceDescription
          )
          checksSuiteResult.checkResults.head.errors.size shouldBe 1
          assertArbCheckErrorsRelateToCorrectDs(checksSuiteResult.checkResults.head.errors.head, ddsPair.datasourceDescription)
        }
      }
    }

    "contains arbitrary checks" should {
      "be able to run completely arbitrary checks and store results in a repository" in {
        val qcResultsRepository = new InMemoryQcResultsRepository

        val arbitraryCheck = ArbitraryCheck("some arbitrary check") {
          RawCheckResult(CheckStatus.Error, "The arbitrary check failed!")
        }
        val qualityChecks = ChecksSuite(
          "table A, table B, and table C comparison",
          arbitraryChecks = Seq(arbitraryCheck),
          tags = someTags,
          qcResultsRepository = qcResultsRepository
        )

        for {
          qcResults: ChecksSuiteResult <- qualityChecks.run(now)
          persistedQcResults <- qcResultsRepository.loadAll.map(_.right.get)
        } yield {
          qcResults.timestamp shouldBe now
          qcResults.checkSuiteDescription shouldBe "table A, table B, and table C comparison"
          qcResults.overallStatus shouldBe CheckSuiteStatus.Error

          persistedQcResults.size shouldBe 1
          persistedQcResults.head.timestamp shouldBe now
          persistedQcResults.head.checkSuiteDescription shouldBe "table A, table B, and table C comparison"
          persistedQcResults.head.overallStatus shouldBe CheckSuiteStatus.Error

          checkResultAndPersistedResult(qcResults, persistedQcResults.head)(
            timestamp = now,
            checkSuiteDescription = "table A, table B, and table C comparison",
            checkStatus = CheckSuiteStatus.Error,
            resultDescription = "0 checks were successful. 1 checks gave errors. 0 checks gave warnings",
            checkResults = Seq(
              CheckResult(
                QcType.ArbitraryCheck,
                CheckStatus.Error,
                "The arbitrary check failed!",
                arbitraryCheck.description
              )
            ),
            checkTags = someTags
          )
        }
      }
    }
  }
}
