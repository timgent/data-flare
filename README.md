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
This library hasn't yet been published sorry! Once it has the following will help you get started.

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