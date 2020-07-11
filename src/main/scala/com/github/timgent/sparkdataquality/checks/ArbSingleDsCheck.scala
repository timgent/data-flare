package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.sparkdataquality.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.sparkdataquality.checks.QCCheck.SingleDsCheck
import com.github.timgent.sparkdataquality.checkssuite.DescribedDs
import org.apache.spark.sql.Dataset

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
      override def description: CheckDescription = SimpleCheckDescription(checkDescription)

      override def applyCheck(dataset: DescribedDs): CheckResult =
        check(dataset.ds).withDescription(qcType, description, Some(SingleDsDescription(dataset.description)))
    }
  }
}
