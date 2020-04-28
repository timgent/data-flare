package qualitychecker

import qualitychecker.CheckResultDetails.NoDetails

import scala.collection.mutable.ListBuffer

trait QcResultsRepository {
  def save(qcResults: Seq[ChecksSuiteResult[_]]): Unit
  def loadAll: Seq[ChecksSuiteResult[NoDetails]]
}

class InMemoryQcResultsRepository extends QcResultsRepository {
  val savedResults: ListBuffer[ChecksSuiteResult[CheckResultDetails.NoDetails]] = ListBuffer.empty

  override def save(qcResults: Seq[ChecksSuiteResult[_]]): Unit = {
    savedResults ++= qcResults.map(_.removeDetails)
  }

  override def loadAll: Seq[ChecksSuiteResult[CheckResultDetails.NoDetails]] = savedResults
}

class NullQcResultsRepository extends QcResultsRepository {
  override def save(qcResults: Seq[ChecksSuiteResult[_]]): Unit = {}

  override def loadAll: Seq[ChecksSuiteResult[NoDetails]] = Seq.empty
}