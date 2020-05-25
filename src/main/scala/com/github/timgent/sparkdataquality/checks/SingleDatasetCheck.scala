package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.functions.sum

/**
 * A check to be done on a single dataset
 */
trait SingleDatasetCheck extends QCCheck {
  def description: String

  override def qcType: QcType = QcType.SingleDatasetQualityCheck

  def applyCheck(ds: Dataset[_]): CheckResult
}

object SingleDatasetCheck {
  def apply(checkDescription: String)(check: Dataset[_] => RawCheckResult): SingleDatasetCheck = {
    new SingleDatasetCheck {
      override def description: String = checkDescription

      override def applyCheck(ds: Dataset[_]): CheckResult = check(ds).withDescription(qcType, checkDescription)
    }
  }

  def sumOfValuesCheck[T: Encoder](columnName: String, threshold: AbsoluteThreshold[T]): SingleDatasetCheck = {
    val checkDescription = s"Constraining sum of column $columnName to be within threshold $threshold"
    SingleDatasetCheck(checkDescription) { ds =>
      val sumOfCol = ds.agg(sum(columnName)).as[T].first()
      val withinThreshold = threshold.isWithinThreshold(sumOfCol)
      if (withinThreshold)
        RawCheckResult(CheckStatus.Success, s"Sum of column $columnName was $sumOfCol, which was within the threshold $threshold")
      else
        RawCheckResult(CheckStatus.Error, s"Sum of column $columnName was $sumOfCol, which was outside the threshold $threshold")
    }
  }
}