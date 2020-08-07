package com.github.timgent.dataflare.repository

import com.github.timgent.dataflare.checkssuite.ChecksSuiteResult

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

trait QcResultsRepository {

  /**
    * Save Quality Check results to some repository
    * @param qcResults A list of results
    * @return A Future of Unit
    */
  def save(qcResults: List[ChecksSuiteResult]): Future[Unit]

  def save(qcResult: ChecksSuiteResult): Future[Unit] = save(List(qcResult))

  /**
    * Load all check results in the repository
    * @return
    */
  def loadAll: Future[List[ChecksSuiteResult]]
}

/**
  * In memory storage of QC Results. Not recommended for production use
  */
class InMemoryQcResultsRepository extends QcResultsRepository {
  val savedResults: ListBuffer[ChecksSuiteResult] = ListBuffer.empty

  override def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = {
    savedResults ++= qcResults
    Future.successful({})
  }

  override def loadAll: Future[List[ChecksSuiteResult]] = Future.successful(savedResults.toList)
}

/**
  * Use the NullQcResultsRepository if you don't need to store QC Results
  */
class NullQcResultsRepository extends QcResultsRepository {
  override def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = Future.successful({})

  override def loadAll: Future[List[ChecksSuiteResult]] = Future.successful(List.empty)
}
