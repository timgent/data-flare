package com.github.sparkdataquality

import java.time.Instant

import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import com.github.sparkdataquality.checks.QCCheck.DatasetComparisonCheck.DatasetPair
import com.github.sparkdataquality.checks.QCCheck.{ArbitraryCheck, DatasetComparisonCheck, DeequQCCheck, SingleDatasetCheck}
import com.github.sparkdataquality.checks.{CheckResult, CheckStatus}
import com.github.sparkdataquality.deequ.DeequHelpers.VerificationResultToQualityCheckResult
import com.github.sparkdataquality.sparkdataquality.DeequMetricsRepository
import enumeratum._
import org.apache.spark.sql.Dataset


trait ChecksSuite {
  def run(timestamp: Instant): ChecksSuiteResult

  def checkSuiteDescription: String

  def qcType: QcType
}

object ChecksSuite {

  private def getOverallCheckResultDescription(checkResults: Seq[CheckResult]): String = {
    val successfulCheckCount = checkResults.count(_.status == CheckStatus.Success)
    val erroringCheckCount = checkResults.count(_.status == CheckStatus.Error)
    val warningCheckCount = checkResults.count(_.status == CheckStatus.Warning)
    s"$successfulCheckCount checks were successful. $erroringCheckCount checks gave errors. $warningCheckCount checks gave warnings"
  }

  trait SingleDatasetChecksSuite extends ChecksSuite {
    def dataset: Dataset[_]
  }

  object SingleDatasetChecksSuite {
    def apply(ds: Dataset[_],
              checkDesc: String,
              checks: Seq[SingleDatasetCheck],
              checkTags: Map[String, String],
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): SingleDatasetChecksSuite = {
      new SingleDatasetChecksSuite {
        def run(timestamp: Instant): ChecksSuiteResult = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(dataset))
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags)
        }

        override def dataset: Dataset[_] = ds

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType = QcType.SingleDatasetQualityCheck
      }
    }
  }

  case class CheckSuiteResult(status: CheckSuiteStatus, resultDescription: String)

  trait DatasetComparisonChecksSuite extends ChecksSuite {
    def datasetToCheck: Dataset[_]

    def datasetToCompareTo: Dataset[_]
  }

  object DatasetComparisonChecksSuite {
    def apply(ds: Dataset[_],
              dsToCompare: Dataset[_],
              checkDesc: String,
              checks: Seq[DatasetComparisonCheck],
              checkTags: Map[String, String],
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): DatasetComparisonChecksSuite = {
      new DatasetComparisonChecksSuite {
        override def datasetToCheck: Dataset[_] = ds

        override def datasetToCompareTo: Dataset[_] = dsToCompare

        override def run(timestamp: Instant): ChecksSuiteResult = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(DatasetPair(datasetToCheck, datasetToCompareTo)))
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType = QcType.DatasetComparisonQualityCheck
      }
    }
  }

  trait ArbitraryChecksSuite extends ChecksSuite

  object ArbitraryChecksSuite {
    def apply(checkDesc: String,
              checks: Seq[ArbitraryCheck],
              checkTags: Map[String, String],
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): ArbitraryChecksSuite =
      new ArbitraryChecksSuite {
        override def run(timestamp: Instant): ChecksSuiteResult = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck)
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType = QcType.ArbitraryQualityCheck
      }
  }


  case class DeequChecksSuite(dataset: Dataset[_], checkSuiteDescription: String, deequChecks: Seq[DeequQCCheck],
                              checkTags: Map[String, String]
                               )(implicit deequMetricsRepository: DeequMetricsRepository)
    extends ChecksSuite {
    override def qcType: QcType = QcType.DeequQualityCheck

    override def run(timestamp: Instant): ChecksSuiteResult = {
      val verificationSuite: VerificationRunBuilder = VerificationSuite()
        .onData(dataset.toDF)
        .useRepository(deequMetricsRepository)
        .saveOrAppendResult(ResultKey(timestamp.toEpochMilli))

      verificationSuite.addChecks(deequChecks.map(_.check))
        .run()
        .toCheckSuiteResult(checkSuiteDescription, timestamp, checkTags)
    }
  }

}

sealed trait QcType extends EnumEntry

object QcType extends Enum[QcType] {
  val values = findValues
  case object DeequQualityCheck extends QcType
  case object SingleDatasetQualityCheck extends QcType
  case object DatasetComparisonQualityCheck extends QcType
  case object ArbitraryQualityCheck extends QcType
}