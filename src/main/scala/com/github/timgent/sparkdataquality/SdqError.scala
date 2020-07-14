package com.github.timgent.sparkdataquality

import cats.Show
import com.github.timgent.sparkdataquality.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.sparkdataquality.checks.DatasourceDescription
import com.github.timgent.sparkdataquality.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.sparkdataquality.checkssuite.{DescribedDs, DescribedDsPair}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor

sealed trait SdqError {
  def datasourceDescription: Option[DatasourceDescription]
  def msg: String
  def err: Option[Throwable]
}

object SdqError {
  case class MetricCalculationError(dds: DescribedDs, metricDescriptors: Seq[MetricDescriptor], err: Option[Throwable]) extends SdqError {
    import cats.implicits._
    override def datasourceDescription: Option[SingleDsDescription] = Some(dds.datasourceDescription)

    override def msg: String =
      s"""One of the metrics defined on dataset ${dds.description} could not be calculated
         |Metrics used were:
         |- ${metricDescriptors.map(_.toSimpleMetricDescriptor.show).mkString("\n- ")}""".stripMargin
  }

  case class ArbCheckError(
      datasourceDescription: Option[DatasourceDescription],
      checkDescription: SimpleCheckDescription,
      err: Option[Throwable]
  ) extends SdqError {
    override def msg: String = s"This check failed due to an exception being thrown during evaluation. Cause: ${err.map(_.getMessage)}"
  }

  implicit val sdqErrorShow: Show[SdqError] = Show.show { error =>
    import error._
    s"""datasourceDescription: $datasourceDescription
       |err: ${err.map(_.getMessage)}
       |msg: $msg""".stripMargin
  }
}
