package com.github.timgent.sparkdataquality.generators

import com.github.timgent.sparkdataquality.metrics.SimpleMetricDescriptor
import org.scalacheck.{Arbitrary, Gen}

object Generators {
  implicit val arbSimpleMetricDescriptor: Arbitrary[SimpleMetricDescriptor] = Arbitrary(Gen.resultOf(SimpleMetricDescriptor))
}
