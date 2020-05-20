package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.DatasetComparisonCheck.DatasetPair
import org.apache.spark.sql.Dataset

trait DatasetComparisonCheck extends QCCheck {
  def description: String

  def applyCheck(dsPair: DatasetPair): CheckResult
}

object DatasetComparisonCheck {

  case class DatasetPair(ds: Dataset[_], dsToCompare: Dataset[_])

  def apply(checkDescription: String)(check: DatasetPair => RawCheckResult): DatasetComparisonCheck = {
    new DatasetComparisonCheck {
      override def description: String = checkDescription

      override def applyCheck(dsPair: DatasetPair): CheckResult = {
        check(dsPair).withDescription(checkDescription)
      }
    }
  }
}