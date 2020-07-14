package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.SdqError.ArbSingleDsCheckError
import com.github.timgent.sparkdataquality.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.sparkdataquality.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.sparkdataquality.checks.QCCheck.SingleDsCheck
import com.github.timgent.sparkdataquality.checkssuite.DescribedDs
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}

/**
  * A check to be done on a single dataset
  */
trait ArbSingleDsCheck extends SingleDsCheck {
  def description: CheckDescription

  override def qcType: QcType = QcType.ArbSingleDsCheck

  def applyCheck(ds: DescribedDs): CheckResult
}

object ArbSingleDsCheck {
  def apply(checkDescription: String)(check: Dataset[_] => RawCheckResult): ArbSingleDsCheck = {
    new ArbSingleDsCheck {
      override def description: SimpleCheckDescription = SimpleCheckDescription(checkDescription)

      override def applyCheck(dataset: DescribedDs): CheckResult = {
        val maybeRawCheckResult = Try(check(dataset.ds))
        maybeRawCheckResult match {
          case Failure(exception) =>
            CheckResult(
              qcType,
              CheckStatus.Error,
              "Check failed due to unexpected exception during evaluation",
              description,
              Some(dataset.datasourceDescription),
              errors = Seq(ArbSingleDsCheckError(dataset, description, Some(exception)))
            )
          case Success(rawCheckResult) =>
            rawCheckResult.withDescription(qcType, description, Some(SingleDsDescription(dataset.description)))
        }
      }
    }
  }
}
