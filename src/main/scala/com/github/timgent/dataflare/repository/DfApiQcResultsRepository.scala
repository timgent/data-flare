package com.github.timgent.dataflare.repository

import cats.implicits._
import com.github.timgent.dataflare.checkssuite.ChecksSuiteResult
import com.github.timgent.dataflare.json.CustomEncodings.{checksSuiteResultDecoder, checksSuiteResultEncoder}
import com.github.timgent.dataflare.repository.QcResultsRepoErr.SaveQcResultErr
import io.circe.parser._
import io.circe.syntax._
import sttp.client3._
import sttp.client3.asynchttpclient.future.AsyncHttpClientFutureBackend
import sttp.model.Uri

import scala.concurrent.{ExecutionContext, Future}
class DfApiQcResultsRepository(host: Uri)(implicit val ec: ExecutionContext) extends QcResultsRepository {

  private lazy val backend = AsyncHttpClientFutureBackend()

  /**
    * Save Quality Check results to some repository
    *
    * @param qcResults A list of results
    * @return A Future of Unit
    */
  override def saveV2(qcResults: List[ChecksSuiteResult]): Future[List[Either[QcResultsRepoErr, ChecksSuiteResult]]] = {
    qcResults.traverse(qcResult =>
      basicRequest
        .contentType("application/json")
        .body(qcResult.asJson.noSpaces)
        .post(host.addPath("qcresults"))
        .send(backend)
        .map { res =>
          val mapped: Either[SaveQcResultErr, ChecksSuiteResult] = res.body match {
            case Left(err) => Left(SaveQcResultErr(err))
            case Right(_)  => Right(qcResult)
          }
          mapped
        }
    )
  }

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
