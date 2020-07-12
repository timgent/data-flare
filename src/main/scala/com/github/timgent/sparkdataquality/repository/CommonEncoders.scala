package com.github.timgent.sparkdataquality.repository

import com.github.timgent.sparkdataquality.metrics.SimpleMetricDescriptor
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._

private[repository] object CommonEncoders {
  implicit val metricDescriptorEncoder: Encoder[SimpleMetricDescriptor] = new Encoder[SimpleMetricDescriptor] {
    override def apply(a: SimpleMetricDescriptor): Json = {
      val fields: List[(String, Json)] = List(
        Some("metricName" -> a.metricName.asJson),
        a.filterDescription.map(fd => "filterDescription" -> fd.asJson),
        a.complianceDescription.map(cd => "complianceDescription" -> cd.asJson),
        a.onColumns.map(oc => "onColumns" -> oc.asJson),
        a.onColumn.map(oc => "onColumn" -> oc.asJson)
      ).flatten
      Json.obj(fields: _*)
    }
  }
  implicit val metricDescriptorDecoder: Decoder[SimpleMetricDescriptor] = deriveDecoder[SimpleMetricDescriptor]
}
