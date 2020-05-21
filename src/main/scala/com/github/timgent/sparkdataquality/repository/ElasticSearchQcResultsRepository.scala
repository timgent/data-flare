package com.github.timgent.sparkdataquality.repository

import com.github.timgent.sparkdataquality.checkssuite.ChecksSuiteResult
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Index}
import io.circe.generic.auto._

import scala.concurrent.{ExecutionContext, Future}

/**
 * An ElasticSearch repository for saving QC results to
 * @param client - an elastic4s ElasticSearch client
 * @param index - the name of the index to save QC results to
 * @param ec - the execution context
 */
class ElasticSearchQcResultsRepository(client: ElasticClient,
                                       index: Index)(implicit ec: ExecutionContext) extends QcResultsRepository {

  override def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = {
    client.execute {
      bulk (
        qcResults.map(indexInto(index).doc(_))
      )
    }.map(_ => {})
  }

  override def loadAll: Future[List[ChecksSuiteResult]] = {
    val resp = client.execute {
      search(index) query matchAllQuery
    }
    resp.map(_.result.hits.hits.map(_.to[ChecksSuiteResult]).toList)
  }
}

object ElasticSearchQcResultsRepository {
  def apply(hosts: Seq[String], index: Index)(implicit ec: ExecutionContext): ElasticSearchQcResultsRepository = {
    val hostList = hosts.reduceLeft(_ + "," + _)
    val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties(hostList)))
    new ElasticSearchQcResultsRepository(client, index)
  }
}
