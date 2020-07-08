---
id: qcresultspersistence
title: Persisting results from your checks
sidebar_label: Persisting results from your checks
---
To persist the results from your `ChecksSuite` simply pass in a `QcResultsRepository` to your `ChecksSuite`. You can 
either use one of the provided implementations or extend `QcResultsRepository` if you wish to use some other storage 
solution.

Example:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checkssuite._
import com.github.timgent.sparkdataquality.repository.ElasticSearchQcResultsRepository
import com.sksamuel.elastic4s.Index

import scala.concurrent.ExecutionContext.Implicits.global

// List of ElasticSearch hosts
val myEsHosts = Seq("1.2.3.4")
val elasticSearchQcResultsRepository = ElasticSearchQcResultsRepository(hosts = myEsHosts, index = Index("my_index"))
val checksSuite = ChecksSuite("persistingChecksSuite", qcResultsRepository = elasticSearchQcResultsRepository)
```

Of course in this example you would need to also pass in some checks to perform.

You can also create an `ElasticSearchQcResultsRepository` with custom properties, for example if you wish to use
authentication.
```scala mdoc:compile-only
import com.sksamuel.elastic4s.{ElasticClient, ElasticNodeEndpoint, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient
val hostList = Seq(ElasticNodeEndpoint(protocol = "http", host = "1.2.3.4", port = 9092, prefix = None))
val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties(hostList, options = Map())))
```
Check out the [elastic4s documentation](https://github.com/sksamuel/elastic4s) for more details on constructing a 
client.
