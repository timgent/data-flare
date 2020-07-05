---
id: metricsdualdschecks
title: Metrics based checks on a pair of Datasets
sidebar_label: Metrics based checks on a pair of Datasets
---
## Performing metric based checks on a pair of datasets
To perform metric based checks on a pair of Datasets you will need to pass a 
`dualDatasetMetricChecks` argument to your ChecksSuite.

```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checkssuite._
val checksSuite = ChecksSuite(
  "someChecksSuite", 
  dualDatasetMetricChecks = ???
)
```
You will need to pass in a sequence of `DualDatasetMetricChecks`. A `DualDatasetMetricChecks` class consists of:
1. A pair of `DescribedDataset` (A and B) - contains the Datasets for the checks to be performed on.
2. A list of `DualMetricBasedCheck`.

## How to create a DualMetricBasedCheck

You can create a `DualMetricBasedCheck` by specifying the metric you want to compare for each dataset, and providing
a MetricComparator that describes what comparison to do between the 2 metrics. For example:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checks.metrics.DualMetricBasedCheck
import com.github.timgent.sparkdataquality.metrics.MetricComparator
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor._
DualMetricBasedCheck(SizeMetric(), CountDistinctValuesMetric(onColumns = List("someColumn")), "dsA size is equal to dsB size",
  MetricComparator.metricsAreEqual
)
```

You can create your own `MetricComparator` if you choose, for example:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.metrics.MetricComparator
import com.github.timgent.sparkdataquality.metrics.MetricValue.LongMetric
val metricBIsDoubleMetricA = MetricComparator[LongMetric]("metricBIsDoubleMetricA", (metricA, metricB) => metricB == metricA * 2)
```

## Putting it all together
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checks.metrics.DualMetricBasedCheck
import com.github.timgent.sparkdataquality.checkssuite.{ChecksSuite, DescribedDataset, DualDatasetMetricChecks, SingleDatasetMetricChecks}
import com.github.timgent.sparkdataquality.metrics.MetricComparator
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.{CountDistinctValuesMetric, SizeMetric}
import org.apache.spark.sql.DataFrame
val myDsA: DataFrame = ???
val myDsB: DataFrame = ???
val myDescribedDsA: DescribedDataset = DescribedDataset(myDsA, "myDs")
val myDescribedDsB: DescribedDataset = DescribedDataset(myDsB, "myDs")
val myDualMetricCheck = DualMetricBasedCheck(
  SizeMetric(),
  CountDistinctValuesMetric(onColumns = List("someColumn")),
  "dsA size is equal to dsB size",
  MetricComparator.metricsAreEqual
)
val checksSuite = ChecksSuite(
  "someChecksSuite",
  dualDatasetMetricChecks = Seq(
    DualDatasetMetricChecks(myDescribedDsA, myDescribedDsB, Seq(myDualMetricCheck))
  )
)
```