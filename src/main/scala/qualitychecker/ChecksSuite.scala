package qualitychecker

import java.time.Instant

import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import org.apache.spark.sql.Dataset
import qualitychecker.CheckResultDetails.{DeequCheckSuiteResultDetails, NoDetails}
import qualitychecker.DeequHelpers.VerificationResultToQualityCheckResult
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

  // TODO: For combining check results we could pass in a FinalCheckResultStrategy and default to everything needing to pass?
  object SingleDatasetChecksSuite {
    def apply(ds: Dataset[_],
              checkDesc: String,
              checks: Seq[SingleDatasetCheck]): SingleDatasetChecksSuite[NoDetails] = {
      new SingleDatasetChecksSuite[NoDetails] {
        def run(timestamp: Instant): ChecksSuiteResult[NoDetails] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(dataset))
          val overallCheckStatus = if (checkResults.forall(_.status == CheckStatus.Success))
            CheckSuiteStatus.Success
          else
            CheckSuiteStatus.Error
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, Map.empty, NoDetails)
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
              checks: Seq[DatasetComparisonCheck]): DatasetComparisonChecksSuite[NoDetails] = {
      new DatasetComparisonChecksSuite[NoDetails] {
        override def datasetToCheck: Dataset[_] = ds

        override def datasetToCompareTo: Dataset[_] = dsToCompare

        override def run(timestamp: Instant): ChecksSuiteResult[NoDetails] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(DatasetPair(datasetToCheck, datasetToCompareTo)))
          val overallCheckStatus = if (checkResults.forall(_.status == CheckStatus.Success))
            CheckSuiteStatus.Success
          else
            CheckSuiteStatus.Error
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, Map.empty, NoDetails)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.DatasetComparisonQualityCheck
      }
    }
  }

  trait ArbitraryChecksSuite[T <: CheckResultDetails] extends ChecksSuite[T]

  object ArbitraryChecksSuite {
    def apply(checkDesc: String, checks: Seq[ArbitraryCheck]): ArbitraryChecksSuite[NoDetails] =
      new ArbitraryChecksSuite[NoDetails] {
        override def run(timestamp: Instant): ChecksSuiteResult[NoDetails] = {
          val checkResults: Seq[CheckResult] = checks.map(_.applyCheck)
          val overallCheckStatus = if (checkResults.forall(_.status == CheckStatus.Success))
            CheckSuiteStatus.Success
          else
            CheckSuiteStatus.Error
          ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
            checkResults, timestamp, qcType, Map.empty, NoDetails)
        }

        override def checkSuiteDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.ArbitraryQualityCheck
      }
  }


  case class DeequChecksSuite(dataset: Dataset[_], checkSuiteDescription: String, deequChecks: Seq[DeequQCCheck],
                              tags: Map[String, String] = Map.empty
                               )(implicit deequMetricsRepository: Option[DeequMetricsRepository])
    extends ChecksSuite[DeequCheckSuiteResultDetails] {
    override def qcType: QcType.Value = QcType.DeequQualityCheck

    override def run(timestamp: Instant): ChecksSuiteResult[DeequCheckSuiteResultDetails] = {
      // TODO: Just pass in a repository, and create a repository that doesn't do anything to give option of not storing results
      def useRepoIfAvailable(verificationRunBuilder: VerificationRunBuilder): VerificationRunBuilder = deequMetricsRepository match {
        case Some(repository) => verificationRunBuilder.useRepository(repository).saveOrAppendResult(ResultKey(timestamp.toEpochMilli))
        case None => verificationRunBuilder
      }

      val verificationSuite: VerificationRunBuilder = VerificationSuite()
        .onData(dataset.toDF)

      useRepoIfAvailable(verificationSuite).addChecks(deequChecks.map(_.check))
        .run()
        .toCheckSuiteResult(checkSuiteDescription, timestamp)
    }
  }

}

object QcType extends Enumeration {
  val DeequQualityCheck, SingleDatasetQualityCheck, DatasetComparisonQualityCheck, ArbitraryQualityCheck = Value
}