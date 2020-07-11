---
id: arbchecks
title: Arbitrary checks
sidebar_label: Arbitrary checks
---
Performing arbitrary checks is important because not every data quality check you will want to do can be encompassed
in a metric. Sometimes you will want to do data quality checks that are less performant and more flexible, and
certainly the performance hit can be worth it if you catch many more data quality issues.

There are 3 types of check included in SDQ that let you do arbitrary checks:

1) ArbSingleDsCheck - perform a check on a single dataset
2) ArbDualDsCheck - perform a check on a pair of datasets
3) ArbitraryCheck - perform a check on any arbitrary input

## SingleDatasetCheck
Frequently you will want to make your `ArbSingleDsCheck`s re-usable so that you can use them in a number of places.
The following example shows one way you may want to use them:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checks._
import com.github.timgent.sparkdataquality.checkssuite._
def hasExpectedColumnsCheck(expectedColumnNames: Set[String]) =
  ArbSingleDsCheck(s"columns == $expectedColumnNames") { ds =>
    val hasExpectedColumns = ds.columns.toSet == expectedColumnNames
    if (hasExpectedColumns)
      RawCheckResult(CheckStatus.Success, "ds had expected columns")
    else
      RawCheckResult(CheckStatus.Error, s"ds had columns ${ds.columns.toSet} instead of expected columns")
  }
val dsA = DescribedDs(???, "dsA")
val checksSuite = ChecksSuite(
  "singleDatasetChecksSuite",
  singleDsChecks = Map(
    dsA -> Seq(hasExpectedColumnsCheck(Set("colA", "colB")))
  )
)
```

## DualDatasetCheck
A `ArbDualDsCheck` describes a comparison between 2 datasets that is not based on metrics. For example:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checks._
import com.github.timgent.sparkdataquality.checkssuite._
val columnsMatchCheck = ArbDualDsCheck("check datasets have same columns") { datasetPair =>
  val datasetsHaveSameColumns = datasetPair.ds.columns.toSet == datasetPair.dsToCompare.columns.toSet
  if (datasetsHaveSameColumns)
    RawCheckResult(CheckStatus.Success, "ds and dsToCompare had the same columns")
  else
    RawCheckResult(
      CheckStatus.Error,
      s"ds had columns ${datasetPair.ds.columns.toSet} but dsToCompare had columns ${datasetPair.dsToCompare.columns.toSet}"
    )
}
val dsA = DescribedDs(???, "dsA")
val dsB = DescribedDs(???, "dsB")
val checksSuite = ChecksSuite("dualDatasetChecksSuite", dualDsChecks = Map(
  DescribedDsPair(dsA, dsB) -> Seq(columnsMatchCheck)
))
```

## ArbitraryCheck
An `ArbitraryCheck` is useful when you would like a check included in the output from SDQ, but the other APIs don't
support it. For example where you want to do a comparison between 3 datasets. Wherever possible we suggest avoiding
this API unless you absolutely need it.
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.checks._
import com.github.timgent.sparkdataquality.checkssuite._
def arbitraryCheck(dsA: DescribedDs, dsB: DescribedDs, dsC: DescribedDs) =
  ArbitraryCheck(s"Check ${dsA.description}, ${dsB.description} and ${dsC.description} all have the same columns") {
    val allColumnsMatch = dsA.ds.columns.toSet == dsB.ds.columns.toSet && dsA.ds.columns.toSet == dsC.ds.columns.toSet
    if (allColumnsMatch)
      RawCheckResult(CheckStatus.Success, "all columns matched")
    else
      RawCheckResult(CheckStatus.Error, "not all columns matched")
  }
val dsA = DescribedDs(???, "dsA")
val dsB = DescribedDs(???, "dsB")
val dsC = DescribedDs(???, "dsC")
val checksSuite = ChecksSuite("arbitraryCheckSuite", arbitraryChecks = Seq(arbitraryCheck(dsA, dsB, dsC)))
```