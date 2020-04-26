package qualitychecker

import java.time.Instant

import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import org.apache.spark.sql.Dataset
import qualitychecker.CheckResultDetails.{DeequCheckResultDetails, NoDetails}
import qualitychecker.DeequHelpers.VerificationResultToQualityCheckResult
import qualitychecker.constraint.ConstraintResult
import qualitychecker.constraint.QCConstraint.DatasetComparisonConstraint.DatasetPair
import qualitychecker.constraint.QCConstraint.{ArbitraryConstraint, DatasetComparisonConstraint, DeequQCConstraint, SingleDatasetConstraint}


trait QualityChecks[T <: CheckResultDetails] {
  def run(timestamp: Instant): QualityCheckResult[T]

  def checkDescription: String

  def qcType: QcType.Value
}

object QualityChecks {

  private def getOverallCheckResultDescription(constraintResults: Seq[ConstraintResult]): String = {
    val successfulConstraintCount = constraintResults.count(_.status == CheckStatus.Success)
    val erroringConstraintCount = constraintResults.count(_.status == CheckStatus.Error)
    val warningConstraintCount = constraintResults.count(_.status == CheckStatus.Warning)
    s"$successfulConstraintCount constraints were successful. $erroringConstraintCount constraints gave errors. $warningConstraintCount constraints gave warnings"
  }

  trait SingleDatasetQualityChecks[T <: CheckResultDetails] extends QualityChecks[T] {
    def dataset: Dataset[_]
  }

  // TODO: For combining constraint results we could pass in a FinalCheckResultStrategy and default to everything needing to pass?
  object SingleDatasetQualityChecks {
    def apply(ds: Dataset[_],
              checkDesc: String,
              constraints: Seq[SingleDatasetConstraint]): SingleDatasetQualityChecks[NoDetails] = {
      new SingleDatasetQualityChecks[NoDetails.type] {
        def run(timestamp: Instant): QualityCheckResult[NoDetails.type] = {
          val constraintResults: Seq[ConstraintResult] = constraints.map(_.applyConstraint(dataset))
          val overallCheckStatus = if (constraintResults.forall(_.status == CheckStatus.Success))
            CheckStatus.Success
          else
            CheckStatus.Error
          QualityCheckResult(overallCheckStatus, checkDescription, getOverallCheckResultDescription(constraintResults),
            constraintResults, timestamp, qcType, Map.empty, NoDetails)
        }

        override def dataset: Dataset[_] = ds

        override def checkDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.SingleDatasetQualityCheck
      }
    }
  }

  case class CheckResult(status: CheckStatus.Value, resultDescription: String)

  trait DatasetComparisonQualityChecks[T <: CheckResultDetails] extends QualityChecks[T] {
    def datasetToCheck: Dataset[_]

    def datasetToCompareTo: Dataset[_]
  }

  object DatasetComparisonQualityChecks {
    def apply(ds: Dataset[_],
              dsToCompare: Dataset[_],
              checkDesc: String,
              constraints: Seq[DatasetComparisonConstraint]): DatasetComparisonQualityChecks[NoDetails] = {
      new DatasetComparisonQualityChecks[NoDetails] {
        override def datasetToCheck: Dataset[_] = ds

        override def datasetToCompareTo: Dataset[_] = dsToCompare

        override def run(timestamp: Instant): QualityCheckResult[NoDetails] = {
          val constraintResults: Seq[ConstraintResult] = constraints.map(_.applyConstraint(DatasetPair(datasetToCheck, datasetToCompareTo)))
          val overallCheckStatus = if (constraintResults.forall(_.status == CheckStatus.Success))
            CheckStatus.Success
          else
            CheckStatus.Error
          QualityCheckResult(overallCheckStatus, checkDescription, getOverallCheckResultDescription(constraintResults),
            constraintResults, timestamp, qcType, Map.empty, NoDetails)
        }

        override def checkDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.DatasetComparisonQualityCheck
      }
    }
  }

  trait ArbitraryQualityChecks[T <: CheckResultDetails] extends QualityChecks[T]

  object ArbitraryQualityChecks {
    def apply(checkDesc: String, constraints: Seq[ArbitraryConstraint]): ArbitraryQualityChecks[NoDetails] =
      new ArbitraryQualityChecks[NoDetails] {
        override def run(timestamp: Instant): QualityCheckResult[NoDetails] = {
          val constraintResults: Seq[ConstraintResult] = constraints.map(_.applyConstraint)
          val overallCheckStatus = if (constraintResults.forall(_.status == CheckStatus.Success))
            CheckStatus.Success
          else
            CheckStatus.Error
          QualityCheckResult(overallCheckStatus, checkDescription, getOverallCheckResultDescription(constraintResults),
            constraintResults, timestamp, qcType, Map.empty, NoDetails)
        }

        override def checkDescription: String = checkDesc

        override def qcType: QcType.Value = QcType.ArbitraryQualityCheck
      }
  }


  case class DeequQualityChecks(dataset: Dataset[_], checkDescription: String, deequConstraints: Seq[DeequQCConstraint],
                                tags: Map[String, String] = Map.empty
                               )(implicit deequMetricsRepository: Option[DeequMetricsRepository])
    extends SingleDatasetQualityChecks[DeequCheckResultDetails] {
    override def qcType: QcType.Value = QcType.DeequQualityCheck

    override def run(timestamp: Instant): QualityCheckResult[DeequCheckResultDetails] = {
      def useRepoIfAvailable(verificationRunBuilder: VerificationRunBuilder): VerificationRunBuilder = deequMetricsRepository match {
        case Some(repository) => verificationRunBuilder.useRepository(repository).saveOrAppendResult(ResultKey(timestamp.toEpochMilli))
        case None => verificationRunBuilder
      }

      val verificationSuite: VerificationRunBuilder = VerificationSuite()
        .onData(dataset.toDF)

      useRepoIfAvailable(verificationSuite).addChecks(deequConstraints.map(_.check))
        .run()
        .toQualityCheckResult(checkDescription, timestamp)
    }
  }

}

object QcType extends Enumeration {
  val DeequQualityCheck, SingleDatasetQualityCheck, DatasetComparisonQualityCheck, ArbitraryQualityCheck = Value
}