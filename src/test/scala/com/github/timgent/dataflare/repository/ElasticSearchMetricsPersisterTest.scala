package com.github.timgent.dataflare.repository

import java.time.Instant

import com.github.timgent.dataflare.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.dataflare.checks.{CheckResult, CheckStatus}
import com.github.timgent.dataflare.metrics.MetricDescriptor.SizeMetric
import com.github.timgent.dataflare.metrics.MetricValue.LongMetric
import com.github.timgent.dataflare.metrics.{MetricValue, SimpleMetricDescriptor}
import com.github.timgent.dataflare.utils.CommonFixtures._
import com.sksamuel.elastic4s.testkit.DockerTests
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration._

class ElasticSearchMetricsPersisterTest extends AsyncWordSpec with Matchers with DockerTests with Eventually with EsTestUtils {
  "ElasticSearchQcResultsRepository.save" should {
    val someIndex = "index_name"
    implicit val patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 1 second)

    cleanIndex(someIndex)
    "Append check suite results to the index" in {
      val repo: ElasticSearchMetricsPersister = new ElasticSearchMetricsPersister(client, someIndex)

      val initialResultsToInsert: Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]] = Map(
        SingleDsDescription("dsA") -> Map(
          SizeMetric().toSimpleMetricDescriptor -> LongMetric(1)
        ),
        SingleDsDescription("dsB") -> Map(
          SizeMetric().toSimpleMetricDescriptor -> LongMetric(1)
        )
      )
      val moreResultsToInsert: Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]] =
        Map(
          SingleDsDescription("dsA") -> Map(
            SizeMetric().toSimpleMetricDescriptor -> LongMetric(2)
          ),
          SingleDsDescription("dsC") -> Map(
            SizeMetric().toSimpleMetricDescriptor -> LongMetric(1)
          )
        )

      def storedResultsFut(): Future[
        List[(Instant, Map[SingleDsDescription, Map[SimpleMetricDescriptor, MetricValue]])]
      ] =
        repo.loadAll.map(_.toList)

      for {
        _ <- repo.save(now, initialResultsToInsert)
        _ <- checkStoredResultsAre(storedResultsFut, Map(now -> initialResultsToInsert).toList)
        _ <- repo.save(later, moreResultsToInsert)
        finalAssertion <- checkStoredResultsAre(
          storedResultsFut,
          Map(now -> initialResultsToInsert, later -> moreResultsToInsert).toList
        )
      } yield {
        finalAssertion
      }
    }
  }
}
