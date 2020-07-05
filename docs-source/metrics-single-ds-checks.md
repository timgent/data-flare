---
id: metricssingledschecks
title: Metrics based checks on a single Dataset
sidebar_label: Metrics based checks on a single Dataset
---
## Performing metric based checks on a single dataset
To perform metric based checks on a single Dataset you will need to pass a 
`singleDatasetMetricChecks` argument to your ChecksSuite.

```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checkssuite._
val checksSuite = ChecksSuite(
  "someChecksSuite", 
  singleDatasetMetricChecks = ???
)
```
You will need to pass in a sequence of `SingleDatasetMetricChecks`. A `SingleDatasetMetricChecks` class consists of:
1. A `DescribedDataset` - contains the Dataset for the checks to be performed on, and a user-friendly description of the
dataset which is used in the returned results and for persisting results and metrics.
2. A list of `SingleMetricBasedCheck`.

## How to create a SingleMetricBasedCheck

### Creating a SingleMetricBasedCheck for maximum flexibility
You can create a `SingleMetricBasedCheck` as follows:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checks.metrics._
import com.github.timgent.sparkdataquality.checks.{CheckStatus, RawCheckResult}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SizeMetric
import com.github.timgent.sparkdataquality.metrics.MetricValue.LongMetric
val mySizeCheck = SingleMetricBasedCheck[LongMetric](SizeMetric(), "sizeMetric"){ size =>
  if (size > 0) RawCheckResult(CheckStatus.Success, "Success!") else RawCheckResult(CheckStatus.Error, "No data!")
}
```
When you create a `SingleMetricBasedCheck` from scratch you have complete control over how the check is done and what
result is returned. You can choose from any of the available metrics and write a function that takes that metric value
and returns a `RawCheckResult`.

### Helpers for creating SingleMetricBasedChecks
If you would like to cut down on the verbosity then there are number of helpers you can use. For example the above
could be written:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checks.metrics._
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
SingleMetricBasedCheck.sizeCheck(AbsoluteThreshold(Some(1L), None))
```
An `AbsoluteThreshold` is a convenience for setting a range of acceptable values. In the case of the above the size of
the dataset must be greater than or equal to 1, with no upper limit on the value.

Other available helpers include:
* sizeCheck
* complianceCheck
* distinctValuesCheck
* thresholdBasedCheck

Please check the API docs for the full range of options!

### Putting it all together
```scala mdoc:compile-only
import org.apache.spark.sql.DataFrame
import com.github.timgent.sparkdataquality.checkssuite._
import com.github.timgent.sparkdataquality.checks.metrics._
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
val myDs: DataFrame = ???
val myDescribedDs: DescribedDataset = DescribedDataset(myDs, "myDs")
val mySizeCheck = SingleMetricBasedCheck.sizeCheck(AbsoluteThreshold(Some(1L), None))
val checksSuite = ChecksSuite(
  "someChecksSuite",
  singleDatasetMetricChecks = Seq(
    SingleDatasetMetricChecks(myDescribedDs, Seq(mySizeCheck))
  )
)
```