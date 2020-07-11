package com.github.timgent.sparkdataquality.checks

/**
  * Check result without additional information about datasource and check description
  *
 * @param status - status of the check
  * @param resultDescription - description of the check result
  */
case class RawCheckResult(
    status: CheckStatus,
    resultDescription: String
) {
  def withDescription(qcType: QcType, checkDescription: CheckDescription, datasourceDescription: Option[DatasourceDescription] = None) =
    CheckResult(qcType, status, resultDescription, checkDescription, datasourceDescription)
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
    checkDescription: CheckDescription,
    datasourceDescription: Option[DatasourceDescription] = None
) {
  def withDatasourceDescription(datasourceDescription: DatasourceDescription): CheckResult =
    this.copy(datasourceDescription = Some(datasourceDescription))
}
