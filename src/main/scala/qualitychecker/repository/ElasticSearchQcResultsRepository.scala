package qualitychecker.repository

import java.time.Instant

import com.sksamuel.elastic4s.{ElasticClient, ElasticDsl, ElasticProperties, Index}
import com.sksamuel.elastic4s.http.JavaClient
import qualitychecker.CheckResultDetails.NoDetailsT
import qualitychecker.{CheckResultDetails, CheckSuiteStatus, ChecksSuiteResult, QcType}
import io.circe.generic.auto._
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.ElasticDsl._
import qualitychecker.checks.CheckResult

class ElasticSearchQcResultsRepository(client: ElasticClient, index: Index) extends QcResultsRepository {

  override def save(qcResults: Seq[ChecksSuiteResult[_]]): Unit = {
    client.execute {
      indexInto(index).doc(qcResults.head)
    }
  }

  override def loadAll: Seq[ChecksSuiteResult[NoDetailsT]] = ???
}

object ElasticSearchQcResultsRepository {
  def apply(hosts: Seq[String], index: Index): ElasticSearchQcResultsRepository = {
    val hostList = hosts.reduceLeft(_ + "," + _)
    val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties(hostList)))
    new ElasticSearchQcResultsRepository(client, index)
  }
}

case class Test(a: String)

case class Moo[T <: CheckResultDetails](
                                         overallStatus: CheckSuiteStatus,
                                         checkSuiteDescription: String,
                                         resultDescription: String,
                                         checkResults: Seq[CheckResult],
                                         timestamp: Instant,
                                         checkType: QcType,
                                         checkTags: Map[String, String]
                                       )