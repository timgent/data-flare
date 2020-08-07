package com.github.timgent.dataflare.repository

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite}

import scala.concurrent.Future

trait EsTestUtils {
  self: Matchers with Eventually with AsyncTestSuite =>
  def checkStoredResultsAre[T](getFutResults: () => Future[List[T]], expected: List[T])(implicit
      patienceConfig: PatienceConfig
  ): Future[Assertion] = {
    eventually {
      for {
        storedResults <- getFutResults()
      } yield storedResults should contain theSameElementsAs expected
    }
  }
}
