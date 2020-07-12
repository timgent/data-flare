---
id: metricstracking
title: Track metrics not involved in any checks
sidebar_label: Track metrics not involved in any checks
---
It's possible to store metrics to enable tracking even if they aren't being used in any checks. You will need to
provide a `metricsPersister` to your `ChecksSuite` as well as your `metricsToTrack`. For example:

```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checkssuite._
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.{SizeMetric, SumValuesMetric}
import com.github.timgent.sparkdataquality.metrics.MetricValue.LongMetric
import org.apache.spark.sql.DataFrame

val myDsA: DataFrame = ???
val myDescribedDsA = DescribedDs(myDsA, "myDsA")
val checksSuite = ChecksSuite(
  "someChecksSuite",
  metricsToTrack = Map(
    myDescribedDsA -> Seq(SizeMetric(), SumValuesMetric[LongMetric](onColumn = "number_of_items"))
  )
)
```
