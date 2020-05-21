# Spark Data Quality
A data quality library build with spark and deequ, to give you ultimate flexibility and power in ensuring your data
is of high quality.

# Key Concepts
* A `ChecksSuite` is a suite of checks that perform a given types of checks. The available ChecksSuites currently are:
    * `DeequChecksSuite` - Allows you to run [deequ](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ) 
    checks on a single DataFrame
    * `SingleDatasetChecksSuite` - Allows you to run arbitrary checks on a single DataFrame
    * `DatasetComparisonChecksSuite` - Allows you to run arbitrary checks on a pair of DataFrames
    * `ArbitraryChecksSuite` - Allows you to run any arbitrary checks, even across a number of DataFrames
* A checks suite is made up of a number of `QCCheck`s. QCChecks define a check to do on the data. The types of `QCCheck`s
you can perform aligns with the `ChecksSuite` above:
    * `DeequQCCheck` - wrapper for deequ's `Check` type
    * `SingleDatasetCheck` - a check performed on a single dataset
    * `DatasetComparisonCheck` - a check performed across 2 datasets
    * `ArbitraryCheck` - a completely arbitrary check

# Getting started
Add the following to your dependencies:
```
libraryDependencies += "com.github.timgent" % "spark-data-quality_2.11" % "0.1.2"
```
For other build systems like maven, and to check the latest version go to https://search.maven.org/artifact/com.github.timgent/spark-data-quality_2.11

## ChecksSuites
ChecksSuites let you perform a number of checks of a particular type. Some examples follow:

### Running your ChecksSuite

#### Creating a repository for check results
First create a repository to store the results of your checks:
```
val qcResultsRepository = new InMemoryQcResultsRepository
```

#### Defining and running checks
Then define your checks (see following sections). Finally run your checks.
```
val qcResults: Seq[ChecksSuiteResult[_]] = QualityChecker.doQualityChecks(qualityChecks, qcResultsRepository, now)
```

### Running metric based checks without deequ
We've built in some metric based checks directly to this library due to some limitations with deequ. In time we hope
to cover the majority of functionality deequ provides. Currently with the build in metrics checks you can:

* Efficiently calculate a few types of metrics on your datasets
* Perform checks on the values of those metrics, either checking they are within a certain range on a single dataset,
or comparing the metric values between datasets
* Store metrics using a MetricsPersister. Currently there is an InMemoryMetricsPersister or an 
ElasticSearchMetricsPersister. The advantage of the ElasticSearch persister is that once your metrics are in 
ElasticSearch you can easily use Kibana to graph them over time and set up dashboards to track your metrics

#### Setting up your metrics repository
```
val client = ElasticClient(JavaClient(ElasticProperties(s"http://${sys.env.getOrElse("ES_HOST", "127.0.0.1")}:${sys.env.getOrElse("ES_PORT", "9200")}")))
val metricsPersister = new ElasticSearchMetricsPersister(client, "myMetricsIndex")
```

#### Performing metric based checks
When you define a MetricsBasedChecksSuite you pass both metric checks for individual datasets as well as metric checks
across pairs of datasets. Combining both types of checks in a single ChecksSuite allows us to be more efficient - for
example re-using metrics that are used for multiple checks.

Example (assuming you have 3 datasets in scope, dsA, dsB, and dsC):
```
val sizeMetric = MetricDescriptor.SizeMetricDescriptor()
val dualMetricChecks = Seq(
    DualMetricBasedCheck(sizeMetric, sizeMetric, MetricComparator.metricsAreEqual, "check size metrics are equal")
)
val dualDatasetChecks = Seq(DualDatasetMetricChecks(
    DescribedDataset(dsA, "dsA"),
    DescribedDataset(dsB, "dsB"),
    dualMetricChecks
))
val singleDatasetChecks: Seq[SingleDatasetMetricChecks] = Seq(SingleDatasetMetricChecks(
    DescribedDataset(dsC, "dsC"),
    Seq(SingleMetricBasedCheck.SizeCheck(AbsoluteThreshold(Some(2), Some(2))))
))
val metricsBasedChecksSuite = MetricsBasedChecksSuite("checkSuiteDescription", someTags, singleDatasetChecks,
        dualDatasetChecks, new InMemoryMetricsPersister)
```

### Running checks using deequ

#### Setting up a metrics repository for deequ metrics
Deequ metrics are stored using it's own repository implementation.

Create a repository to store the checks. Please see deequ documentation for options here. In this repo we also include
a DeequNullMetricsRepository. If you want to run deequ checks but not store the results of the metrics please use this.
```
val deequMetricsRepository = new InMemoryMetricsRepository
```

#### Defining deequ quality checks
Create the quality checks you want to run (simple example given here):
```
val deequQcConstraints = Seq(DeequQCCheck(Check(CheckLevel.Error, "size check").hasSize(_ == 3)))
val qualityChecks = List(DeequChecksSuite(testDataset, "sample deequ checks", deequQcConstraints, Map("tag" -> "tagValue"))(deequMetricsRepository))
```

### Running custom checks on a single dataset
This example returns a RawCheckResult staright away - this is where code would go to do the check you want.
```
val singleDatasetCheck = SingleDatasetCheck("someSingleDatasetCheck") {
    dataset => RawCheckResult(CheckStatus.Error, "someSingleDatasetCheck was not successful")
}
val qualityChecks = List(SingleDatasetChecksSuite(testDataset, checkDescription, Seq(singleDatasetCheck), someTags))
```

### Running custom checks on a pair of datasets
This example returns a RawCheckResult staright away - this is where code would go to do the check you want.
```
val datasetComparisonCheck = DatasetComparisonCheck("Table counts equal") { case DatasetPair(ds, dsToCompare) =>
    RawCheckResult(CheckStatus.Error, "counts were not equal")
}
    val qualityChecks = Seq(DatasetComparisonChecksSuite(testDataset, datasetToCompare, "table A vs table B comparison", Seq(datasetComparisonCheck), someTags))
```

### Running custom checks for anything more complex
This example returns a RawCheckResult staright away - this is where code would go to do the check you want.
```
val arbitraryCheck = ArbitraryCheck("some arbitrary check") {
    RawCheckResult(CheckStatus.Error, "The arbitrary check failed!")
}
val qualityChecks = Seq(ArbitraryChecksSuite("table A, table B, and table C comparison", Seq(arbitraryCheck), someTags))
```

## Published with SBT Sonatype
https://github.com/xerial/sbt-sonatype