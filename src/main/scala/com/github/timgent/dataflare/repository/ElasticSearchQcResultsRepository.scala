package com.github.timgent.dataflare.repository

import com.github.timgent.dataflare.checkssuite.ChecksSuiteResult
import com.github.timgent.dataflare.json.CustomEncodings.{checksSuiteResultDecoder, checksSuiteResultEncoder}
import com.github.timgent.dataflare.repository.QcResultsRepoErr.SaveQcResultErr
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Index, RequestFailure, RequestSuccess}

import scala.concurrent.{ExecutionContext, Future}

/**
  * An ElasticSearch repository for saving QC results to
  * @param client - an elastic4s ElasticSearch client
  * @param index - the name of the index to save QC results to
  * @param ec - the execution context
  */
class ElasticSearchQcResultsRepository(client: ElasticClient, index: Index)(implicit val ec: ExecutionContext) extends QcResultsRepository {
  override def saveV2(qcResults: List[ChecksSuiteResult]): Future[List[Either[QcResultsRepoErr, ChecksSuiteResult]]] = {
    client
      .execute {
        bulk(
          qcResults.map(indexInto(index).doc(_))
        )
      }
      .map {
        case RequestSuccess(status, body, headers, result) => qcResults.map(Right(_))
        case RequestFailure(status, body, headers, error)  => List(Left(SaveQcResultErr(error.reason)))
      }
  }

  override def loadAll: Future[List[ChecksSuiteResult]] = {
    val resp = client.execute {
      search(index) query matchAllQuery
    }
    resp.map(_.result.hits.hits.map(_.to[ChecksSuiteResult]).toList)
  }
}

object ElasticSearchQcResultsRepository {
  def apply(hosts: Seq[String], index: Index)(implicit
      ec: ExecutionContext
  ): ElasticSearchQcResultsRepository = {
    val hostList = hosts.reduceLeft(_ + "," + _)
    val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties(hostList)))
    new ElasticSearchQcResultsRepository(client, index)
  }
}
