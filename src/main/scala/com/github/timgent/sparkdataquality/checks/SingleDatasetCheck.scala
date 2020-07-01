package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.sparkdataquality.checkssuite.DescribedDataset
import org.apache.spark.sql.Dataset

/**
  * A check to be done on a single dataset
  */
trait SingleDatasetCheck extends QCCheck {
  def description: String

  override def qcType: QcType = QcType.SingleDatasetQualityCheck

  def applyCheck(ds: DescribedDataset): CheckResult
}

object SingleDatasetCheck {
  def apply(checkDescription: String)(check: Dataset[_] => RawCheckResult): SingleDatasetCheck = {
    new SingleDatasetCheck {
      override def description: String = checkDescription

      override def applyCheck(dataset: DescribedDataset): CheckResult =
        check(dataset.ds).withDescription(qcType, checkDescription, Some(SingleDsDescription(dataset.description)))
    }
  }
}
