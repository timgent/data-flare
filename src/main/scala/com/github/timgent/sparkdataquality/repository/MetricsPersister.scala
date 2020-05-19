package com.github.timgent.sparkdataquality.repository

import java.time.Instant

import com.github.timgent.sparkdataquality.metrics.{DatasetDescription, MetricValue, SimpleMetricDescriptor}

import scala.collection.mutable
import scala.concurrent.Future

trait MetricsPersister {
  def save(timestamp: Instant,
           metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
                  ): Future[Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]

  def loadAll: Future[Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]]
}

object NullMetricsPersister extends MetricsPersister {
  override def save(timestamp: Instant,
                    metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
                           ): Future[Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
    Future.successful(metrics)
  }

  override def loadAll: Future[Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]] = Future.successful(Map.empty)
}

class InMemoryMetricsPersister extends MetricsPersister {

  val savedResults: mutable.Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] = mutable.Map.empty

  override def save(timestamp: Instant,
                    metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
                           ): Future[Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
    savedResults += (timestamp -> metrics)
    Future.successful(metrics)
  }

  override def loadAll: Future[Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]] = {
    Future.successful(savedResults.toMap)
  }
}
