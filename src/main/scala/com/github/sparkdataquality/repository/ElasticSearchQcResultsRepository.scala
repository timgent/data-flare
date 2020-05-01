package com.github.sparkdataquality.repository

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Index}
import io.circe.generic.auto._
import com.github.sparkdataquality.ChecksSuiteResult

class ElasticSearchQcResultsRepository(client: ElasticClient, index: Index) extends QcResultsRepository {

  // TODO: Change repository methods to be asynchronous
  override def save(qcResults: Seq[ChecksSuiteResult]): Unit = {
    client.execute {
      bulk (
        qcResults.map(indexInto(index).doc(_))
      )
    }.await
  }

  // TODO: Change repository methods to be asynchronous
  override def loadAll: Seq[ChecksSuiteResult] = {
    val resp = client.execute {
      search(index) query matchAllQuery
    }.await
    resp.result.hits.hits.map(_.to[ChecksSuiteResult])
  }
}

object ElasticSearchQcResultsRepository {
  def apply(hosts: Seq[String], index: Index): ElasticSearchQcResultsRepository = {
    val hostList = hosts.reduceLeft(_ + "," + _)
    val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties(hostList)))
    new ElasticSearchQcResultsRepository(client, index)
  }
}
