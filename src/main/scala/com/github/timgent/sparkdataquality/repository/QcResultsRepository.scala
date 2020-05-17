package com.github.timgent.sparkdataquality.repository

import com.github.timgent.sparkdataquality.checkssuite.ChecksSuiteResult

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

trait QcResultsRepository {
  def save(qcResults: List[ChecksSuiteResult]): Future[Unit]
  def loadAll: Future[List[ChecksSuiteResult]]
}

class InMemoryQcResultsRepository extends QcResultsRepository {
  val savedResults: ListBuffer[ChecksSuiteResult] = ListBuffer.empty

  override def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = {
    savedResults ++= qcResults.map(_.removeDetails)
    Future.successful({})
  }

  override def loadAll: Future[List[ChecksSuiteResult]] = Future.successful(savedResults.toList)
}

class NullQcResultsRepository extends QcResultsRepository {
  override def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = Future.successful({})

  override def loadAll: Future[List[ChecksSuiteResult]] = Future.successful(List.empty)
}

