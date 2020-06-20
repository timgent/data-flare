package com.github.timgent.sparkdataquality.repository

import java.time.Instant

import com.github.timgent.sparkdataquality.metrics.{
  DatasetDescription,
  MetricValue,
  SimpleMetricDescriptor
}
import com.sksamuel.elastic4s.ElasticDsl.{bulk, indexInto, _}
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Index}
import io.circe.generic.auto._

import scala.concurrent.{ExecutionContext, Future}
private case class EsMetricsDocument(
    timestamp: Instant,
    datasetDescription: String,
    metricDescriptor: SimpleMetricDescriptor,
    metricValue: MetricValue
)

private object EsMetricsDocument {
  def fromMetricsMap(
      timestamp: Instant,
      metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
  ): List[EsMetricsDocument] = {
    (for {
      (datasetDescription, metricsMap) <- metrics
      (metricDescriptor, metricValue) <- metricsMap
    } yield EsMetricsDocument(
      timestamp,
      datasetDescription.value,
      metricDescriptor,
      metricValue
    )).toList
  }
  def docsToMetricsMap(
      documents: Seq[EsMetricsDocument]
  ): Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
    documents
      .groupBy(_.timestamp)
      .mapValues(
        _.groupBy(doc => DatasetDescription(doc.datasetDescription))
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
      metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
  ): Future[Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
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

  override def loadAll
      : Future[Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]] = {
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
