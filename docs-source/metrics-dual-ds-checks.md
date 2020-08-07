---
id: metricsdualdschecks
title: Metrics based checks on a pair of Datasets
sidebar_label: Metrics based checks on a pair of Datasets
---
## Performing metric based checks on a pair of datasets
To perform metric based checks on a pair of Datasets you will need to pass a 
`dualDsChecks` argument to your ChecksSuite.

```scala mdoc:compile-only
import com.github.timgent.dataflare.checkssuite._
val checksSuite = ChecksSuite(
  "someChecksSuite", 
  dualDsChecks = ???
)
```
You will need to pass in a `Map[DescribedDsPair, DualDsCheck]`. The keys represent the pair of datasets that they 
checks will be performed on. The values represent the checks to be done on the pair of datasets. 

## How to create a DualMetricCheck

You can create a `DualMetricCheck` by specifying the metric you want to compare for each dataset, and providing
a MetricComparator that describes what comparison to do between the 2 metrics. For example:
```scala mdoc:compile-only
import com.github.timgent.dataflare.checks.metrics.DualMetricCheck
import com.github.timgent.dataflare.metrics.MetricComparator
import com.github.timgent.dataflare.metrics.MetricDescriptor._
DualMetricCheck(SizeMetric(), CountDistinctValuesMetric(onColumns = List("someColumn")), "dsA size is equal to dsB size",
  MetricComparator.metricsAreEqual
)
```

You can create your own `MetricComparator` if you choose, for example:
```scala mdoc:compile-only
import com.github.timgent.dataflare.metrics.MetricComparator
import com.github.timgent.dataflare.metrics.MetricValue.LongMetric
val metricBIsDoubleMetricA = MetricComparator[LongMetric]("metricBIsDoubleMetricA", (metricA, metricB) => metricB == metricA * 2)
```

## Putting it all together
```scala mdoc:compile-only
import com.github.timgent.dataflare.checks.metrics.DualMetricCheck
import com.github.timgent.dataflare.checkssuite.{ChecksSuite, DescribedDs, DescribedDsPair}
import com.github.timgent.dataflare.metrics.MetricComparator
import com.github.timgent.dataflare.metrics.MetricDescriptor.{CountDistinctValuesMetric, SizeMetric}
import org.apache.spark.sql.DataFrame
val myDsA: DataFrame = ???
val myDsB: DataFrame = ???
val myDescribedDsA: DescribedDs = DescribedDs(myDsA, "myDs")
val myDescribedDsB: DescribedDs = DescribedDs(myDsB, "myDs")
val myDualMetricCheck = DualMetricCheck(
  SizeMetric(),
  CountDistinctValuesMetric(onColumns = List("someColumn")),
  "dsA size is equal to dsB size",
  MetricComparator.metricsAreEqual
)
val checksSuite = ChecksSuite(
  "someChecksSuite",
  dualDsChecks = Map(
    DescribedDsPair(myDescribedDsA, myDescribedDsB) -> Seq(myDualMetricCheck)
  )
)
```