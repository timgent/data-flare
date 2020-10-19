---
id: metricssingledschecks
title: Metrics based checks on a single Dataset
sidebar_label: Metrics based checks on a single Dataset
---
## Performing metric based checks on a single dataset
To perform metric based checks on a single Dataset you will need to pass a 
`singleDsChecks` argument to your ChecksSuite.

```scala mdoc:compile-only
import com.github.timgent.dataflare.checkssuite._
val checksSuite = ChecksSuite(
  "someChecksSuite", 
  singleDsChecks = ???
)
```
You will need to pass in a `Map[DescribedDataset, Seq[SingleDsCheck]]`.

## How to create a SingleMetricCheck

### Creating a SingleMetricCheck for maximum flexibility
You can create a `SingleMetricCheck` as follows:
```scala mdoc:compile-only
import com.github.timgent.dataflare.checks.metrics._
import com.github.timgent.dataflare.checks.{CheckStatus, RawCheckResult}
import com.github.timgent.dataflare.metrics.MetricDescriptor.SizeMetric
import com.github.timgent.dataflare.metrics.MetricValue.LongMetric
val mySizeCheck = SingleMetricCheck[LongMetric](SizeMetric(), "sizeMetric"){ size =>
  if (size > 0) RawCheckResult(CheckStatus.Success, "Success!") else RawCheckResult(CheckStatus.Error, "No data!")
}
```
When you create a `SingleMetricCheck` from scratch you have complete control over how the check is done and what
result is returned. You can choose from any of the available metrics and write a function that takes that metric value
and returns a `RawCheckResult`.

### Helpers for creating SingleMetricChecks
If you would like to cut down on the verbosity then there are number of helpers you can use. For example the above
could be written:
```scala mdoc:compile-only
import com.github.timgent.dataflare.checks.metrics._
import com.github.timgent.dataflare.thresholds.AbsoluteThreshold
SingleMetricCheck.sizeCheck(AbsoluteThreshold(Some(1L), None))
```
An `AbsoluteThreshold` is a convenience for setting a range of acceptable values. In the case of the above the size of
the dataset must be greater than or equal to 1, with no upper limit on the value.

Other available helpers include:
* sizeCheck
* complianceCheck
* distinctValuesCheck
* distinctnessCheck
* thresholdBasedCheck

Please check the API docs for the full range of options!

### Putting it all together
```scala mdoc:compile-only
import org.apache.spark.sql.DataFrame
import com.github.timgent.dataflare.checkssuite._
import com.github.timgent.dataflare.checks.metrics._
import com.github.timgent.dataflare.thresholds.AbsoluteThreshold
val myDs: DataFrame = ???
val myDescribedDs: DescribedDs = DescribedDs(myDs, "myDs")
val mySizeCheck = SingleMetricCheck.sizeCheck(AbsoluteThreshold(Some(1L), None))
val checksSuite = ChecksSuite(
  "someChecksSuite",
  singleDsChecks = Map(
    myDescribedDs -> Seq(mySizeCheck)
  )
)
```