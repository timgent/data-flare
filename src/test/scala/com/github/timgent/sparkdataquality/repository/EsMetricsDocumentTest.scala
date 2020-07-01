package com.github.timgent.sparkdataquality.repository

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.arbInstantJdk8
import com.github.timgent.sparkdataquality.checks.DatasourceDescription.SingleDsDescription
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.sparkdataquality.metrics.{MetricValue, SimpleMetricDescriptor}
import com.github.timgent.sparkdataquality.utils.CommonFixtures._
import io.circe.parser._
import io.circe.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class EsMetricsDocumentTest extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  private implicit val arbSimpleMetricDescriptor: Arbitrary[SimpleMetricDescriptor] = Arbitrary(Gen.resultOf(SimpleMetricDescriptor))
  private val longMetricGen = Gen.resultOf(LongMetric)
  private val doubleMetricGen = Gen.resultOf(DoubleMetric)
  private implicit val arbMetricValue: Arbitrary[MetricValue] = Arbitrary(Gen.oneOf(longMetricGen, doubleMetricGen))
  implicit private val esMetricsDocumentArb = Arbitrary(for {
    timestamp <- arbInstantJdk8.arbitrary
    datasetDescription <- arbString.arbitrary.map(SingleDsDescription)
    metricDescriptor <- arbitrary[SimpleMetricDescriptor]
    metricValue <- arbitrary[MetricValue]
  } yield EsMetricsDocument(timestamp, datasetDescription, metricDescriptor, metricValue))

  "EsMetricsDocument" should {
    "be encoded in JSON as expected" in {
      val json = EsMetricsDocument(
        now,
        SingleDsDescription("myDs"),
        SimpleMetricDescriptor("myMetric", Some("myFilter"), None, None),
        LongMetric(10)
      ).asJson
      val expectedJson = parse(
        s"""
          |{
          | "timestamp" : "${now.toString}",
          | "datasourceDescription" : "myDs",
          | "metricDescriptor" : {
          |   "metricName" : "myMetric",
          |   "filterDescription" : "myFilter"
          | },
          | "metricValue" : {
          |   "type": "LongMetric",
          |   "value": 10
          | }
          |}""".stripMargin
      ).right.get
      json shouldBe expectedJson
    }

    "always be possible to encode and decode from JSON" in {
      forAll { esMetricsDocument: EsMetricsDocument =>
        val afterRoundTrip = esMetricsDocument.asJson.as[EsMetricsDocument]
        afterRoundTrip.right.get shouldBe esMetricsDocument
      }
    }
  }
}
