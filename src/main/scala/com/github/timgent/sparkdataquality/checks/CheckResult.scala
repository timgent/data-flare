package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.metrics.DatasetDescription

/**
 * Check result without additional information about datasource and check description
 * @param status - status of the check
 * @param resultDescription - description of the check result
 */
private [sparkdataquality] case class RawCheckResult(
                        status: CheckStatus,
                        resultDescription: String
                      ) {
  def withDescription(qcType: QcType, checkDescription: String) = CheckResult(qcType, status, resultDescription, checkDescription)
}

/**
 * The result of a check
 * @param status - status of the check
 * @param resultDescription - description of the check result
 * @param checkDescription - description of the check
 * @param datasourceDescription - optional description of the datasource used in the check
 */
case class CheckResult(
                        qcType: QcType,
                        status: CheckStatus,
                        resultDescription: String,
                        checkDescription: String,
                        datasourceDescription: Option[String] = None
                      ) {
  def withDatasourceDescription(datasetDescription: DatasetDescription): CheckResult =
    this.copy(datasourceDescription = Some(datasetDescription.value))
  def withDatasourceDescription(datasourceDescription: String): CheckResult =
    this.copy(datasourceDescription = Some(datasourceDescription))
}
