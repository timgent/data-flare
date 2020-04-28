package qualitychecker

import java.time.Instant

import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import org.apache.spark.sql.Dataset
import qualitychecker.CheckResultDetails.{DeequCheckSuiteResultDetails, NoDetails}
import qualitychecker.deequ.DeequHelpers.VerificationResultToQualityCheckResult
import qualitychecker.checks.{CheckResult, CheckStatus}
import qualitychecker.checks.QCCheck.DatasetComparisonCheck.DatasetPair
import qualitychecker.checks.QCCheck.{ArbitraryCheck, DatasetComparisonCheck, DeequQCCheck, SingleDatasetCheck}


trait ChecksSuite[T <: CheckResultDetails] {
  def run(timestamp: Instant): ChecksSuiteResult[T]

  def checkSuiteDescription: String

  def qcType: QcType.Value
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
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus.Value = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): SingleDatasetChecksSuite[NoDetails] = {
      new SingleDatasetChecksSuite[NoDetails] {
        def run(timestamp: Instant): ChecksSuiteResult[NoDetails] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(dataset))
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags, NoDetails)
        }

        override def dataset: Dataset[_] = ds

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.SingleDatasetQualityCheck
      }
    }
  }

  case class CheckSuiteResult(status: CheckSuiteStatus.Value, resultDescription: String)

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
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus.Value = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): DatasetComparisonChecksSuite[NoDetails] = {
      new DatasetComparisonChecksSuite[NoDetails] {
        override def datasetToCheck: Dataset[_] = ds

        override def datasetToCompareTo: Dataset[_] = dsToCompare

        override def run(timestamp: Instant): ChecksSuiteResult[NoDetails] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(DatasetPair(datasetToCheck, datasetToCompareTo)))
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags, NoDetails)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.DatasetComparisonQualityCheck
      }
    }
  }

  trait ArbitraryChecksSuite[T <: CheckResultDetails] extends ChecksSuite[T]

  object ArbitraryChecksSuite {
    def apply(checkDesc: String,
              checks: Seq[ArbitraryCheck],
              checkTags: Map[String, String],
              checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus.Value = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
             ): ArbitraryChecksSuite[NoDetails] =
      new ArbitraryChecksSuite[NoDetails] {
        override def run(timestamp: Instant): ChecksSuiteResult[NoDetails] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck)
          val overallCheckStatus = checkResultCombiner(checkResults)
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, checkTags, NoDetails)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.ArbitraryQualityCheck
      }
  }


  case class DeequChecksSuite(dataset: Dataset[_], checkSuiteDescription: String, deequChecks: Seq[DeequQCCheck],
                              checkTags: Map[String, String]
                               )(implicit deequMetricsRepository: DeequMetricsRepository)
    extends ChecksSuite[DeequCheckSuiteResultDetails] {
    override def qcType: QcType.Value = QcType.DeequQualityCheck

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

object QcType extends Enumeration {
  val DeequQualityCheck, SingleDatasetQualityCheck, DatasetComparisonQualityCheck, ArbitraryQualityCheck = Value
}