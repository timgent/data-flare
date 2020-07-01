package com.github.timgent.sparkdataquality.repository

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.sparkdataquality.checkssuite.{CheckSuiteStatus, ChecksSuiteResult}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Index}
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import cats.syntax.either._

import scala.concurrent.{ExecutionContext, Future}

/**
  * An ElasticSearch repository for saving QC results to
  * @param client - an elastic4s ElasticSearch client
  * @param index - the name of the index to save QC results to
  * @param ec - the execution context
  */
class ElasticSearchQcResultsRepository(client: ElasticClient, index: Index)(implicit
    ec: ExecutionContext
) extends QcResultsRepository {
  import ElasticSearchQcResultsRepository.checksSuiteResultEncoder
  import ElasticSearchQcResultsRepository.checksSuiteResultDecoder

  override def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = {
    client
      .execute {
        bulk(
          qcResults.map(indexInto(index).doc(_))
        )
      }
      .map(_ => {})
  }

  override def loadAll: Future[List[ChecksSuiteResult]] = {
    val resp = client.execute {
      search(index) query matchAllQuery
    }
    resp.map(_.result.hits.hits.map(_.to[ChecksSuiteResult]).toList)
  }
}

object ElasticSearchQcResultsRepository {
  import io.circe.generic.semiauto._
  private implicit val checkSuiteStatusEncoder: Encoder[CheckSuiteStatus] = new Encoder[CheckSuiteStatus] {
    override def apply(a: CheckSuiteStatus): Json = Json.fromString(a.toString)
  }
  private implicit val qcTypeEncoder: Encoder[QcType] = new Encoder[QcType] {
    override def apply(a: QcType): Json = Json.fromString(a.toString)
  }
  private implicit val checkStatusEncoder: Encoder[CheckStatus] = new Encoder[CheckStatus] {
    override def apply(a: CheckStatus): Json = Json.fromString(a.toString)
  }
  private implicit val checkResultEncoder: Encoder[CheckResult] = deriveEncoder[CheckResult]
  private implicit val checkResultDecoder: Decoder[CheckResult] = new Decoder[CheckResult] {
    override def apply(c: HCursor): Result[CheckResult] = {
      for {
        qcType <- c.downField("qcType").as[String].map(QcType.namesToValuesMap)
        status <- c.downField("status").as[String].map(CheckStatus.namesToValuesMap)
        resultDescription <- c.downField("resultDescription").as[String]
        checkDescription <- c.downField("checkDescription").as[String]
        datasourceDescription = c.downField("datasourceDescription").as[String].fold(_ => None, Some(_))
      } yield CheckResult(qcType, status, resultDescription, checkDescription, datasourceDescription)
    }
  }
  private[repository] implicit val checksSuiteResultEncoder: Encoder[ChecksSuiteResult] = deriveEncoder[ChecksSuiteResult]
  private[repository] implicit val checksSuiteResultDecoder: Decoder[ChecksSuiteResult] = new Decoder[ChecksSuiteResult] {
    override def apply(c: HCursor): Result[ChecksSuiteResult] = {
      for {
        overallStatus <- c.downField("overallStatus").as[String].map(CheckSuiteStatus.namesToValuesMap)
        checkSuiteDescription <- c.downField("checkSuiteDescription").as[String]
        checkResults <- c.downField("checkResults").as[Seq[CheckResult]]
        timestamp <- c.downField("timestamp").as[Instant]
        checkTags <- c.downField("checkTags").as[Map[String, String]]
      } yield ChecksSuiteResult(overallStatus, checkSuiteDescription, checkResults, timestamp, checkTags)
    }
  }

  def apply(hosts: Seq[String], index: Index)(implicit
      ec: ExecutionContext
  ): ElasticSearchQcResultsRepository = {
    val hostList = hosts.reduceLeft(_ + "," + _)
    val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties(hostList)))
    new ElasticSearchQcResultsRepository(client, index)
  }
}
