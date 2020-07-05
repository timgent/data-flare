---
id: firstchecks
title: Writing your first suite of checks
sidebar_label: Writing your first suite of checks
---
## Introduction to ChecksSuite
The entry point for any SDQ job is a `ChecksSuite`. You can pass in some metadata about the checksuite, details of the 
checks to perform, your repositories for storing metrics and results, and rules about how to calculate an overall 
check status. For example:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checkssuite.ChecksSuite
val myFirstChecksSuite = ChecksSuite(
    checkSuiteDescription = "myFirstChecksSuite",
    tags = ???,
    singleDatasetMetricChecks = ???,
    dualDatasetMetricChecks = ???,
    singleDatasetChecks = ???,
    dualDatasetChecks = ???,
    arbitraryChecks = ???,
    deequChecks = ???,
    metricsPersister = ???,
    deequMetricsRepository = ???,
    qcResultsRepository = ???,
    checkResultCombiner = ???
)
```

Check out the [API docs](/spark-data-quality/api/index.html) for full details of the arguments for a ChecksSuite. The most important thing
to know is that all of these arguments except for checkSuiteDescription are optional. We recommend just specifying the
items you are interested in. Where you don't provide arguments either no checks of that type will be run or the metrics
or the QC Results won't be stored. The default value for `tags` is an empty map. The default `checkResultCombiner` will
use the worst status for any individual checks as the overall status for the `ChecksSuiteResult`.

## A simple ChecksSuite
Let's look at a simple example where we run some performant metric-based checks a single Dataset.
```scala mdoc:compile-only
  import java.time.Instant

  import com.github.timgent.sparkdataquality.checks.metrics.SingleMetricBasedCheck
  import com.github.timgent.sparkdataquality.checkssuite._
  import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.{Dataset, SparkSession}

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.Future
  val sparkConf = new SparkConf().setAppName("SimpleChecksSuite").setMaster("local")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  import spark.implicits._
  case class NumberString(num: Int, string: String)
  val ds: Dataset[NumberString] = List(
    NumberString(1, "a"),
    NumberString(2, "b"),
    NumberString(3, "c")
  ).toDS
  val numberStrings: DescribedDataset = DescribedDataset(ds, "numberStrings")
  val simpleChecksSuite: ChecksSuite = ChecksSuite(
    checkSuiteDescription = "simpleChecksSuite",
    singleDatasetMetricChecks = Seq(
      SingleDatasetMetricChecks(
        numberStrings,
        Seq(
          SingleMetricBasedCheck.sizeCheck(AbsoluteThreshold(3, 5)),
          SingleMetricBasedCheck.distinctValuesCheck(AbsoluteThreshold(2, 5), List("num")))
      )
    )
  )
  val qcResults: Future[ChecksSuiteResult] = simpleChecksSuite.run(Instant.now)
```
In this case we're defining a ChecksSuite that does just 2 checks, both on the same Dataset. One checks the size
of the Dataset is between 3 and 5, and the other checks that there are between 2 and 5 distinct numbers. Because no
repository is provided for the results, and no Persister is provided for the metrics, no persistence of results or
metrics will take place.

You'll find some more details about the different types of checks you can do, and how to persist your results and your
metrics in the other sections of the documentation.