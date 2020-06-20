package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checkssuite.DescribedDatasetPair
import org.apache.spark.sql.Dataset

/**
  * Check for comparing a pair of datasets
  */
trait DatasetComparisonCheck extends QCCheck {
  def description: String

  override def qcType: QcType = QcType.DatasetComparisonQualityCheck

  def applyCheck(dsPair: DescribedDatasetPair): CheckResult
}

object DatasetComparisonCheck {

  case class DatasetPair(ds: Dataset[_], dsToCompare: Dataset[_])

  def apply(
      checkDescription: String
  )(check: DatasetPair => RawCheckResult): DatasetComparisonCheck = {
    new DatasetComparisonCheck {
      override def description: String = checkDescription

      override def applyCheck(dsPair: DescribedDatasetPair): CheckResult = {
        check(dsPair.rawDatasetPair)
          .withDescription(qcType, checkDescription, dsPair.datasourceDescription)
      }
    }
  }
}
