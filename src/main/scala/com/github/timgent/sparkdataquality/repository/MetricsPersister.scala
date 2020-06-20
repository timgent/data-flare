package com.github.timgent.sparkdataquality.repository

import java.time.Instant

import com.github.timgent.sparkdataquality.metrics.{DatasetDescription, MetricValue, SimpleMetricDescriptor}

import scala.collection.mutable
import scala.concurrent.Future

trait MetricsPersister {

  /**
    * Save the given metrics to some storage layer depending on implementation chosen
    * @param timestamp - the timestamp to associate the metrics to
    * @param metrics - a map with an entry for each dataset. An inner map for each type of metric for that dataset.
    * @return returns a future of the passed metrics
    */
  def save(
      timestamp: Instant,
      metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
  ): Future[Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]

  /**
    * Loads all metrics in the repository
    * @return Future of a map with timestamps to metrics
    */
  def loadAll: Future[Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]]
}

/**
  * Does not persist metrics. Will always return an empty map if used to load metrics. Use this when you don't want to
  * persist metrics
  */
object NullMetricsPersister extends MetricsPersister {
  override def save(
      timestamp: Instant,
      metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
  ): Future[Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
    Future.successful(metrics)
  }

  override def loadAll: Future[Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]] =
    Future.successful(Map.empty)
}

/**
  * An in memory metrics persister. Not recommended for production use
  */
class InMemoryMetricsPersister extends MetricsPersister {

  val savedResults: mutable.Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] =
    mutable.Map.empty

  override def save(
      timestamp: Instant,
      metrics: Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]
  ): Future[Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]] = {
    savedResults += (timestamp -> metrics)
    Future.successful(metrics)
  }

  override def loadAll: Future[Map[Instant, Map[DatasetDescription, Map[SimpleMetricDescriptor, MetricValue]]]] = {
    Future.successful(savedResults.toMap)
  }
}
