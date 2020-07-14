package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.SdqError.ArbCheckError
import com.github.timgent.sparkdataquality.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.sparkdataquality.checks.QCCheck.DualDsQCCheck
import com.github.timgent.sparkdataquality.checkssuite.DescribedDsPair
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}

/**
  * Check for comparing a pair of datasets
  */
trait ArbDualDsCheck extends DualDsQCCheck {
  def description: CheckDescription

  override def qcType: QcType = QcType.ArbDualDsCheck

  def applyCheck(dsPair: DescribedDsPair): CheckResult
}

object ArbDualDsCheck {

  case class DatasetPair(ds: Dataset[_], dsToCompare: Dataset[_])

  def apply(
      checkDescription: String
  )(check: DatasetPair => RawCheckResult): ArbDualDsCheck = {
    new ArbDualDsCheck {
      override def description: SimpleCheckDescription = SimpleCheckDescription(checkDescription)

      override def applyCheck(ddsPair: DescribedDsPair): CheckResult = {
        val maybeRawCheckResult = Try(check(ddsPair.rawDatasetPair))
        maybeRawCheckResult match {
          case Failure(exception) =>
            CheckResult(
              qcType,
              CheckStatus.Error,
              "Check failed due to unexpected exception during evaluation",
              description,
              Some(ddsPair.datasourceDescription),
              errors = Seq(ArbCheckError(Some(ddsPair.datasourceDescription), description, Some(exception)))
            )
          case Success(rawCheckResult) => rawCheckResult.withDescription(qcType, description, Some(ddsPair.datasourceDescription))
        }
      }
    }
  }
}
