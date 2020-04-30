package qualitychecker.repository

import qualitychecker.CheckResultDetails.NoDetailsT
import qualitychecker.{CheckResultDetails, ChecksSuiteResult}

import scala.collection.mutable.ListBuffer

trait QcResultsRepository {
  def save(qcResults: Seq[ChecksSuiteResult[_]]): Unit
  def loadAll: Seq[ChecksSuiteResult[NoDetailsT]]
}

class InMemoryQcResultsRepository extends QcResultsRepository {
  val savedResults: ListBuffer[ChecksSuiteResult[CheckResultDetails.NoDetailsT]] = ListBuffer.empty

  override def save(qcResults: Seq[ChecksSuiteResult[_]]): Unit = {
    savedResults ++= qcResults.map(_.removeDetails)
  }

  override def loadAll: Seq[ChecksSuiteResult[CheckResultDetails.NoDetailsT]] = savedResults
}

class NullQcResultsRepository extends QcResultsRepository {
  override def save(qcResults: Seq[ChecksSuiteResult[_]]): Unit = {}

  override def loadAll: Seq[ChecksSuiteResult[NoDetailsT]] = Seq.empty
}

