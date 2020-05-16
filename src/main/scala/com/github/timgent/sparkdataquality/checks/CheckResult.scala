package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.metrics.DatasetDescription

case class RawCheckResult(
                        status: CheckStatus,
                        resultDescription: String
                      ) {
  def withDescription(checkDescription: String) = CheckResult(status, resultDescription, checkDescription)
}

case class CheckResult(
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
