package qualitychecker

import qualitychecker.CheckResultDetails.NoDetails

import scala.collection.mutable.ListBuffer

trait QcResultsRepository {
  def save(qcResults: Seq[QualityCheckResult[_]]): Unit
  def loadAll: Seq[QualityCheckResult[NoDetails.type]]
}

class InMemoryQcResultsRepository extends QcResultsRepository {
  val savedResults: ListBuffer[QualityCheckResult[CheckResultDetails.NoDetails.type]] = ListBuffer.empty

  override def save(qcResults: Seq[QualityCheckResult[_]]): Unit = {
    savedResults ++= qcResults.map(_.removeDetails)
  }

  override def loadAll: Seq[QualityCheckResult[CheckResultDetails.NoDetails.type]] = savedResults
}
