package com.github.timgent.dataflare

import cats.Show
import com.github.timgent.dataflare.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.dataflare.checks.DatasourceDescription
import com.github.timgent.dataflare.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.dataflare.checkssuite.{DescribedDs, DescribedDsPair}
import com.github.timgent.dataflare.metrics.MetricDescriptor

sealed trait FlareError {
  def datasourceDescription: Option[DatasourceDescription]
  def msg: String
  def err: Option[Throwable]
}

object FlareError {
  case class MetricCalculationError(dds: DescribedDs, metricDescriptors: Seq[MetricDescriptor], err: Option[Throwable]) extends FlareError {
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
  ) extends FlareError {
    override def msg: String = s"This check failed due to an exception being thrown during evaluation. Cause: ${err.map(_.getMessage)}"
  }

  implicit val FlareErrorShow: Show[FlareError] = Show.show { error =>
    import error._
    s"""datasourceDescription: $datasourceDescription
       |err: ${err.map(_.getMessage)}
       |msg: $msg""".stripMargin
  }

  sealed trait MetricLookupError extends FlareError

  case object MetricMissing extends MetricLookupError {
    override def datasourceDescription: Option[DatasourceDescription] = None

    override def msg: String = "Unexpected failure - Metric lookup failed - no metric found - please raise an Issue on Github"

    override def err: Option[Throwable] = None
  }

  case object LookedUpMetricOfWrongType extends MetricLookupError {
    override def datasourceDescription: Option[DatasourceDescription] = None

    override def msg: String =
      "Unexpected failure - Metric lookup failed - metric found was of wrong type - please raise an Issue on Github"

    override def err: Option[Throwable] = None
  }
}
