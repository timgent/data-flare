---
id: availablemetrics
title: Available metrics
sidebar_label: Available metrics
---
The available metrics are:
* `SizeMetric` - count the number of rows in your dataset
* `SumValuesMetric` - sum up a given column in your dataset
* `CountDistinctValuesMetric` - count the distinct values across a given set of columns
* `ComplianceMetric` - calculate the fraction of rows that comply with the given condition

With most metrics a filter can be applied before the metric gets calculated - you can see an example of this below. 

## Size Metric
Size metrics are very straightforward and just count the number of rows in your dataset. A filter can be applied
to the dataset before the rows are counted.
e.g.
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SizeMetric
import com.github.timgent.sparkdataquality.metrics.MetricFilter
import org.apache.spark.sql.functions.col

SizeMetric(MetricFilter(col("fullName").isNotNull, "fullName is not null"))
```
A MetricFilter takes a filter condition and a descriptive string which is used for persistence.

## SumValuesMetric
SumValuesMetric sums the values in a single column. A filter can be provided (the default is no filter)
e.g.
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SumValuesMetric
import com.github.timgent.sparkdataquality.metrics.MetricFilter
import com.github.timgent.sparkdataquality.metrics.MetricValue.LongMetric

SumValuesMetric[LongMetric]("numberOfItems", MetricFilter.noFilter)
```
Note the type parameter. This is required to specify the type that should be used for the metric. For example if you
have fractional values you should use a `DoubleMetric`, but for whole numbers a `LongMetric` is more appropriate.

## ComplianceMetric
ComplianceMetric tells you what proportion of rows in a dataset comply with the given constraint. Again a filter can
be applied before the metric is calculated. For example:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.metrics.{ComplianceFn, MetricFilter}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.ComplianceMetric
import org.apache.spark.sql.functions.col

ComplianceMetric(ComplianceFn(col("fullName").isNotNull, "fullName is not null"), MetricFilter.noFilter)
```

## CountDistinctValuesMetric
CountDistinctValuesMetric counts the number of distinct values in the given columns. A filter can be applied before the
metric is calculated. For example:
```scala mdoc:compile-only
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.CountDistinctValuesMetric
import com.github.timgent.sparkdataquality.metrics.MetricFilter

CountDistinctValuesMetric(List("firstName", "surname"), MetricFilter.noFilter)
```