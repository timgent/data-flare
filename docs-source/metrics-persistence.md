---
id: metricspersistence
title: Persisting metrics over time
sidebar_label: Persisting metrics over time
---
To persist the metrics from your `ChecksSuite` simply pass in a `MetricsPersister` to your `ChecksSuite`. You can 
either use one of the provided implementations or extend `MetricsPersister` if you wish to use some other storage 
solution.

Example:
```scala mdoc:compile-only
import com.github.timgent.dataflare.checkssuite.ChecksSuite
import com.github.timgent.dataflare.repository.ElasticSearchMetricsPersister
import com.sksamuel.elastic4s.Index

import scala.concurrent.ExecutionContext.Implicits.global

val esHosts = Seq("1.2.3.4")
val esMetricsPersister = ElasticSearchMetricsPersister(esHosts, Index("metrics_index"))
val checksSuite = ChecksSuite("metricsPersister", metricsPersister = esMetricsPersister)
```

As per [qc results persistence](./qc-results-persistence.md) you can also pass an `ElasticClient` to instantiate an
`ElasticSearchMetricsPersister` if you wish to set custom properties like authentication for your ElasticSearch
cluster.