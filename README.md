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
