package com.github.timgent.sparkdataquality.repository

import java.time.Instant

import com.github.timgent.sparkdataquality.metrics.{DatasetDescription, MetricDescriptor, MetricValue}

import scala.concurrent.Future
import scala.collection.mutable

trait MetricsPersister {
  def storeMetrics(timestamp: Instant,
                   metrics: Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]
                  ): Future[Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]]

  def getAllMetrics: Map[Instant, Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]]
}

object NullMetricsPersister extends MetricsPersister {
  override def storeMetrics(timestamp: Instant,
                            metrics: Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]
                           ): Future[Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]] = {
    Future.successful(metrics)
  }

  override def getAllMetrics: Map[Instant, Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]] = Map.empty
}

class InMemoryMetricsPersister extends MetricsPersister {

  val savedResults: mutable.Map[Instant, Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]] = mutable.Map.empty

  override def storeMetrics(timestamp: Instant,
                            metrics: Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]
                           ): Future[Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]] = {
    savedResults += (timestamp -> metrics)
    Future.successful(metrics)
  }

  override def getAllMetrics: Map[Instant, Map[DatasetDescription, Map[MetricDescriptor, MetricValue]]] = {
    savedResults.toMap
  }
}
