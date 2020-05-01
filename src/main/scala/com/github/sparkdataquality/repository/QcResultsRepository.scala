package com.github.sparkdataquality.repository

import com.github.sparkdataquality.ChecksSuiteResult

import scala.collection.mutable.ListBuffer

trait QcResultsRepository {
  def save(qcResults: Seq[ChecksSuiteResult]): Unit
  def loadAll: Seq[ChecksSuiteResult]
}

class InMemoryQcResultsRepository extends QcResultsRepository {
  val savedResults: ListBuffer[ChecksSuiteResult] = ListBuffer.empty

  override def save(qcResults: Seq[ChecksSuiteResult]): Unit = {
    savedResults ++= qcResults.map(_.removeDetails)
  }

  override def loadAll: Seq[ChecksSuiteResult] = savedResults
}

class NullQcResultsRepository extends QcResultsRepository {
  override def save(qcResults: Seq[ChecksSuiteResult]): Unit = {}

  override def loadAll: Seq[ChecksSuiteResult] = Seq.empty
}

