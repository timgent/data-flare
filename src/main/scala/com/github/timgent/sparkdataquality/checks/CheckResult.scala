package com.github.timgent.sparkdataquality.checks

import cats.Show
import com.github.timgent.sparkdataquality.SdqError

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
  * @param errors - any errors that occured when trying to execute the check
  */
case class CheckResult(
    qcType: QcType,
    status: CheckStatus,
    resultDescription: String,
    checkDescription: CheckDescription,
    datasourceDescription: Option[DatasourceDescription] = None,
    errors: Seq[SdqError] = Seq.empty
) {
  def withDatasourceDescription(datasourceDescription: DatasourceDescription): CheckResult =
    this.copy(datasourceDescription = Some(datasourceDescription))
}

object CheckResult {
  implicit val showCheckResult: Show[CheckResult] = Show.show { checkResult =>
    import checkResult._
    import cats.implicits._
    s"""checkDescription -> ${checkDescription.show}
       |resultDescription -> $resultDescription
       |status -> $status
       |datasourceDescription -> $datasourceDescription
       |qcType -> $qcType
       |errors -> 
       |  ${errors.map(_.show.replaceAll("\n", "\n  ")).mkString("\n")}
       |""".stripMargin
  }
}
