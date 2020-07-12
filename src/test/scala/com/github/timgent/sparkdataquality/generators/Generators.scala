package com.github.timgent.sparkdataquality.generators

import com.github.timgent.sparkdataquality.metrics.SimpleMetricDescriptor
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

object Generators {
  implicit val arbSimpleMetricDescriptor: Arbitrary[SimpleMetricDescriptor] = Arbitrary(for {
    metricName <- arbitrary[String]
    filterDescription <- arbitrary[Option[String]]
    complianceDescription <- arbitrary[Option[String]]
    onColumns <- arbitrary[Option[List[String]]]
    onColumn <- arbitrary[Option[String]]
  } yield SimpleMetricDescriptor(metricName, filterDescription, complianceDescription, onColumns, onColumn))
}
