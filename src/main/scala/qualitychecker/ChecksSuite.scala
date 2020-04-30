package qualitychecker

import java.time.Instant

import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import enumeratum._
import org.apache.spark.sql.Dataset
import qualitychecker.CheckResultDetails.{DeequCheckSuiteResultDetails, NoDetails, NoDetailsT}
import qualitychecker.deequ.DeequHelpers.VerificationResultToQualityCheckResult
import qualitychecker.checks.{CheckResult, CheckStatus}
import qualitychecker.checks.QCCheck.DatasetComparisonCheck.DatasetPair
import qualitychecker.checks.QCCheck.{ArbitraryCheck, DatasetComparisonCheck, DeequQCCheck, SingleDatasetCheck}


trait ChecksSuite[T <: CheckResultDetails] {
  def run(timestamp: Instant): ChecksSuiteResult[T]

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

  trait SingleDatasetChecksSuite[T <: CheckResultDetails] extends ChecksSuite[T] {
    def dataset: Dataset[_]
  }

  object SingleDatasetChecksSuite {
    def apply(ds: Dataset[_],
              checkDesc: String,
              checks: Seq[SingleDatasetCheck],
              checkTags: Map[String, String],
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): SingleDatasetChecksSuite[NoDetailsT] = {
      new SingleDatasetChecksSuite[NoDetailsT] {
        def run(timestamp: Instant): ChecksSuiteResult[NoDetailsT] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(dataset))
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags, NoDetails)
        }

        override def dataset: Dataset[_] = ds

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType = QcType.SingleDatasetQualityCheck
      }
    }
  }

  case class CheckSuiteResult(status: CheckSuiteStatus, resultDescription: String)

  trait DatasetComparisonChecksSuite[T <: CheckResultDetails] extends ChecksSuite[T] {
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
             ): DatasetComparisonChecksSuite[NoDetailsT] = {
      new DatasetComparisonChecksSuite[NoDetailsT] {
        override def datasetToCheck: Dataset[_] = ds

        override def datasetToCompareTo: Dataset[_] = dsToCompare

        override def run(timestamp: Instant): ChecksSuiteResult[NoDetailsT] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(DatasetPair(datasetToCheck, datasetToCompareTo)))
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags, NoDetails)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType = QcType.DatasetComparisonQualityCheck
      }
    }
  }

  trait ArbitraryChecksSuite[T <: CheckResultDetails] extends ChecksSuite[T]

  object ArbitraryChecksSuite {
    def apply(checkDesc: String,
              checks: Seq[ArbitraryCheck],
              checkTags: Map[String, String],
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): ArbitraryChecksSuite[NoDetailsT] =
      new ArbitraryChecksSuite[NoDetailsT] {
        override def run(timestamp: Instant): ChecksSuiteResult[NoDetailsT] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck)
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags, NoDetails)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType = QcType.ArbitraryQualityCheck
      }
  }


  case class DeequChecksSuite(dataset: Dataset[_], checkSuiteDescription: String, deequChecks: Seq[DeequQCCheck],
                              checkTags: Map[String, String]
                               )(implicit deequMetricsRepository: DeequMetricsRepository)
    extends ChecksSuite[DeequCheckSuiteResultDetails] {
    override def qcType: QcType = QcType.DeequQualityCheck

    override def run(timestamp: Instant): ChecksSuiteResult[DeequCheckSuiteResultDetails] = {
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