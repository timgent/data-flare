package com.github.timgent.sparkdataquality.checks

sealed trait DatasourceDescription

object DatasourceDescription {
  case class SingleDsDescription(datasource: String) extends DatasourceDescription
  case class DualDsDescription(datasourceA: String, datasourceB: String) extends DatasourceDescription
  case class OtherDsDescription(datasource: String) extends DatasourceDescription
}
