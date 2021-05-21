package com.github.timgent.dataflare.generators

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.arbInstantJdk8
import com.github.timgent.dataflare.checks.CheckDescription.{DualMetricCheckDescription, SimpleCheckDescription, SingleMetricCheckDescription}
import com.github.timgent.dataflare.checks.DatasourceDescription.{DualDsDescription, OtherDsDescription, SingleDsDescription}
import com.github.timgent.dataflare.checks._
import com.github.timgent.dataflare.checkssuite.{CheckSuiteStatus, ChecksSuiteResult}
import com.github.timgent.dataflare.metrics.SimpleMetricDescriptor
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant

object Generators {
  implicit val arbSimpleMetricDescriptor: Arbitrary[SimpleMetricDescriptor] = Arbitrary(for {
    metricName <- arbitrary[String]
    filterDescription <- arbitrary[Option[String]]
    complianceDescription <- arbitrary[Option[String]]
    onColumns <- arbitrary[Option[List[String]]]
    onColumn <- arbitrary[Option[String]]
  } yield SimpleMetricDescriptor(metricName, filterDescription, complianceDescription, onColumns, onColumn))
  implicit val checkSuiteStatusArb = Arbitrary(Gen.oneOf(CheckSuiteStatus.values))
  implicit val checkStatusArb = Arbitrary(Gen.oneOf(CheckStatus.values))
  implicit val simpleCheckDescriptionArb = Arbitrary(Gen.resultOf(SimpleCheckDescription))
  implicit val dualMetricCheckDescriptionArb = Arbitrary(Gen.resultOf(DualMetricCheckDescription))
  implicit val singleMetricCheckDescriptionArb = Arbitrary(Gen.resultOf(SingleMetricCheckDescription))
  implicit val checkDescriptionArb: Arbitrary[CheckDescription] = Arbitrary(
    Gen.oneOf(arbitrary[SimpleCheckDescription], arbitrary[DualMetricCheckDescription], arbitrary[SingleMetricCheckDescription])
  )
  implicit val qcTypeArb = Arbitrary(Gen.oneOf(QcType.values))
  implicit val singleDsDescriptionArb = Arbitrary(Gen.resultOf(SingleDsDescription))
  implicit val dualDsDescriptionArb = Arbitrary(Gen.resultOf(DualDsDescription))
  implicit val otherDsDescriptionArb = Arbitrary(Gen.resultOf(OtherDsDescription))
  implicit val datasourceDescriptionArb: Arbitrary[Option[DatasourceDescription]] =
    Arbitrary(Gen.option(Gen.oneOf(arbitrary[SingleDsDescription], arbitrary[DualDsDescription], arbitrary[OtherDsDescription])))
  implicit val checkResultArb = Arbitrary(for {
    qcType <- arbitrary[QcType]
    status <- arbitrary[CheckStatus]
    resultDescription <- arbitrary[String]
    checkDescription <- arbitrary[CheckDescription]
    datasourceDescription <- arbitrary[Option[DatasourceDescription]]
  } yield CheckResult(qcType, status, resultDescription, checkDescription, datasourceDescription))
  val checksSuiteResultGen: Gen[ChecksSuiteResult] = for {
    overallStatus <- arbitrary[CheckSuiteStatus]
    checkSuiteDescription <- arbitrary[String]
    checkResults <- arbitrary[Seq[CheckResult]]
    timestamp <- arbitrary[Instant]
    checkTags <- arbitrary[Map[String, String]]
  } yield ChecksSuiteResult(overallStatus, checkSuiteDescription, checkResults, timestamp, checkTags)
  implicit val checksSuiteResultArb: Arbitrary[ChecksSuiteResult] = Arbitrary(checksSuiteResultGen)

}
