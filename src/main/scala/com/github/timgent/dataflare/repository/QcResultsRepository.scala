package com.github.timgent.dataflare.repository

import com.github.timgent.dataflare.checkssuite.{ChecksSuiteErr, ChecksSuiteResult}
import com.github.timgent.dataflare.repository.QcResultsRepoErr.{LoadQcResultErr, QcResultsRepoException}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

trait QcResultsRepository {
  implicit def ec: ExecutionContext

  /**
    * Save Quality Check results to some repository. Will replace save method over time
    * @param qcResults A list of results
    * @return A Future of either an error or unit
    */
  def saveV2(qcResults: List[ChecksSuiteResult]): Future[List[Either[QcResultsRepoErr, ChecksSuiteResult]]]

  /**
    * Save Quality Check results to some repository
    * @param qcResults A list of results
    * @return A Future of Unit
    */
  @deprecated("will be replaced by saveV2 method which has better error handling", "Jul 2021")
  def save(qcResults: List[ChecksSuiteResult]): Future[Unit] = saveV2(qcResults).map(_ => ())

  def saveV2(qcResult: ChecksSuiteResult): Future[Either[QcResultsRepoErr, ChecksSuiteResult]] =
    saveV2(List(qcResult)).map(_.head)

  @deprecated("will be replaced by saveV2 method which has better error handling", "Jul 2021")
  def save(qcResult: ChecksSuiteResult): Future[Unit] = save(List(qcResult))

  /**
    * Load all check results in the repository
    * @return
    */
  def loadAll: Future[Either[LoadQcResultErr, List[ChecksSuiteResult]]]
}

sealed trait QcResultsRepoErr extends ChecksSuiteErr {
  def throwErr: Nothing =
    e match {
      case Some(e) => throw new QcResultsRepoException(err, e)
      case None    => throw new QcResultsRepoException(err)
    }
  def err: String
  def e: Option[Throwable]
}

object QcResultsRepoErr {

  class QcResultsRepoException(msg: String) extends Exception(msg) {
    def this(message: String, cause: Throwable) {
      this(message)
      initCause(cause)
    }
  }

  /**
    * Represents an error that occurred when saving QC Results
    * @param err describes the error that was encountered
    */
  case class SaveQcResultErr(err: String) extends QcResultsRepoErr {
    override def e: Option[Throwable] = None
  }
  case class LoadQcResultErr(err: String, e: Option[Throwable]) extends QcResultsRepoErr
}

/**
  * In memory storage of QC Results. Not recommended for production use
  */
class InMemoryQcResultsRepository extends QcResultsRepository {
  override implicit def ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  val savedResults: ListBuffer[ChecksSuiteResult] = ListBuffer.empty

  override def saveV2(qcResults: List[ChecksSuiteResult]): Future[List[Either[QcResultsRepoErr, ChecksSuiteResult]]] = {
    savedResults ++= qcResults
    Future.successful(qcResults.map(Right(_)))
  }

  override def loadAll: Future[Either[LoadQcResultErr, List[ChecksSuiteResult]]] = Future.successful(Right(savedResults.toList))
}

/**
  * Use the NullQcResultsRepository if you don't need to store QC Results
  */
class NullQcResultsRepository extends QcResultsRepository {
  override implicit def ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  override def saveV2(qcResults: List[ChecksSuiteResult]): Future[List[Either[QcResultsRepoErr, ChecksSuiteResult]]] =
    Future.successful(qcResults.map(Right(_)))

  override def loadAll: Future[Either[LoadQcResultErr, List[ChecksSuiteResult]]] = Future.successful(Right(List.empty))
}
