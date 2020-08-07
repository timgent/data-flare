package com.github.timgent.dataflare.repository

import java.time.Instant

import cats.syntax.either._
import com.github.timgent.dataflare.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.dataflare.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.dataflare.metrics.{MetricValue, SimpleMetricDescriptor}
import com.sksamuel.elastic4s.ElasticDsl.{bulk, indexInto, _}
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Index}
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.{Encoder, _}

import scala.concurrent.{ExecutionContext, Future}
private[repository] case class EsMetricsDocument(
    timestamp: Instant,
    datasourceDescription: SingleDsDescription,
    metricDescriptor: SimpleMetricDescriptor,
    metricValue: MetricValue
)

private object EsMetricsDocument {
  import CommonEncoders.{metricDescriptorEncoder, metricDescriptorDecoder}
  private implicit val metricValueEncoder: Encoder[MetricValue] = new Encoder[MetricValue] {
    override def apply(a: MetricValue): Json = {
      a match {
        case LongMetric(value)               => Json.obj("value" -> value.asJson, "type" -> Json.fromString("LongMetric"))
        case MetricValue.DoubleMetric(value) => Json.obj("value" -> value.asJson, "type" -> Json.fromString("DoubleMetric"))
      }
    }
  }
  private implicit val metricValueDecoder: Decoder[MetricValue] = new Decoder[MetricValue] {
    override def apply(c: HCursor): Decoder.Result[MetricValue] = {
      for {
        metricType <- c.downField("type").as[String]
        value = c.downField("value")
        metricValue <- metricType match {
          case "LongMetric"   => value.as[Long].map(LongMetric)
          case "DoubleMetric" => value.as[Double].map(DoubleMetric)
        }
      } yield metricValue
    }
  }
  private implicit val singleDsDescriptionEncoder = Encoder.encodeString.contramap[SingleDsDescription](_.datasource)
  private implicit val singleDsDescriptionDecoder = Decoder.decodeString.emap(datasource => Right(SingleDsDescription(datasource)))
  implicit val encoder: Encoder[EsMetricsDocument] = deriveEncoder[EsMetricsDocument]
  implicit val decoder: Decoder[EsMetricsDocument] = deriveDecoder[EsMetricsDocument]

  def fromMetricsMap(
      timestamp: Instant,
      metrics: Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]]
  ): List[EsMetricsDocument] = {
    (for {
      (datasetDescription, metricsMap) <- metrics
      (metricDescriptor, metricValue) <- metricsMap
    } yield EsMetricsDocument(
      timestamp,
      datasetDescription,
      metricDescriptor,
      metricValue
    )).toList
  }
  def docsToMetricsMap(
      documents: Seq[EsMetricsDocument]
  ): Map[Instant, Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
    documents
      .groupBy(_.timestamp)
      .mapValues(
        _.groupBy(doc => doc.datasourceDescription)
          .mapValues(
            _.groupBy(_.metricDescriptor)
              .mapValues(_.head.metricValue)
          )
      ) // .head is safe as there is guaranteed to be a metric value
  }
}

/**
  *
  * @param client elastic4s ElasticSearch client
  * @param index the name of the index to save metrics to
  * @param ec the execution context
  */
class ElasticSearchMetricsPersister(client: ElasticClient, index: Index)(implicit
    ec: ExecutionContext
) extends MetricsPersister {
  override def save(
      timestamp: Instant,
      metrics: Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]]
  ): Future[Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
    client
      .execute {
        bulk(
          EsMetricsDocument
            .fromMetricsMap(timestamp, metrics)
            .map(metric => indexInto(index).doc(metric))
        )
      }
      .map(_ => metrics)
  }

  override def loadAll: Future[Map[Instant, Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]]]] = {
    val resp = client.execute {
      search(index) query matchAllQuery
    }
    val esMetricDocuments: Future[List[EsMetricsDocument]] =
      resp.map(_.result.hits.hits.map(_.to[EsMetricsDocument]).toList)
    esMetricDocuments.map(EsMetricsDocument.docsToMetricsMap)
  }
}

object ElasticSearchMetricsPersister {
  def apply(hosts: Seq[String], index: Index)(implicit
      ec: ExecutionContext
  ): ElasticSearchMetricsPersister = {
    val hostList = hosts.reduceLeft(_ + "," + _)
    val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties(hostList)))
    new ElasticSearchMetricsPersister(client, index)
  }
}
