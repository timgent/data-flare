package com.github.timgent.dataflare.repository

import cats.implicits._
import com.github.timgent.dataflare.checkssuite.ChecksSuiteResult
import com.github.timgent.dataflare.json.CustomEncodings.{checksSuiteResultDecoder, checksSuiteResultEncoder}
import io.circe.parser._
import io.circe.syntax._
import sttp.client3._
import sttp.client3.asynchttpclient.future.AsyncHttpClientFutureBackend
import sttp.model.Uri

import scala.concurrent.{ExecutionContext, Future}
class DfApiQcResultsRepository(host: Uri)(implicit
    ec: ExecutionContext
) extends QcResultsRepository {

  private lazy val backend = AsyncHttpClientFutureBackend()

  /**
    * Save Quality Check results to some repository
    *
    * @param qcResults A list of results
    * @return A Future of Unit
    */
  override def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = {
    qcResults.traverse(qcResult =>
      basicRequest
        .contentType("application/json")
        .body(qcResult.asJson.noSpaces)
        .post(host.addPath("qcresults"))
        .send(backend)
        .map(_ => ())
    )
  }.map(_ => ())

  /**
    * Load all check results in the repository
    *
    * @return
    */
  override def loadAll: Future[List[ChecksSuiteResult]] =
    basicRequest
      .contentType("application/json")
      .get(host.addPath("qcresults"))
      .send(backend)
      .map { r =>
        val o: Either[String, String] = r.body
        val p = parse(o.right.get).right
        val pp = p.get.as[List[ChecksSuiteResult]]
        val ppp = pp.right.get
        ppp
      }
}
