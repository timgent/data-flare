package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.QCCheck.DualDsQCCheck
import com.github.timgent.sparkdataquality.checkssuite.DescribedDsPair
import org.apache.spark.sql.Dataset

/**
  * Check for comparing a pair of datasets
  */
trait ArbDualDsCheck extends DualDsQCCheck {
  def description: String

  override def qcType: QcType = QcType.DatasetComparisonQualityCheck

  def applyCheck(dsPair: DescribedDsPair): CheckResult
}

object ArbDualDsCheck {

  case class DatasetPair(ds: Dataset[_], dsToCompare: Dataset[_])

  def apply(
      checkDescription: String
  )(check: DatasetPair => RawCheckResult): ArbDualDsCheck = {
    new ArbDualDsCheck {
      override def description: String = checkDescription

      override def applyCheck(dsPair: DescribedDsPair): CheckResult = {
        check(dsPair.rawDatasetPair)
          .withDescription(qcType, checkDescription, dsPair.datasourceDescription)
      }
    }
  }
}
