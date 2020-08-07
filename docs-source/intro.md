---
id: intro
title: Introduction to Data Flare
sidebar_label: Introduction
---

Data Flare (Flare for short) is a data quality library built on top of [Apache Spark](https://spark.apache.org/), and enables
you assure the data quality of large scale datasets, both by providing fast feedback on the quality of your data, and
by enabling you to easily store and visualize key metrics for your data and track them over time.

## How does it work?
* You define checks in Scala code, giving you complete flexibility with the checks you want to define.
  * Checks can be based on metrics associated with the data, such as the size of a dataset, level of compliance with a
particular condition, etc. Flare runs these checks efficiently, using a single pass over the data to calculate all
metrics.
  * Checks can be completely arbitrary, enabling you to do more complex checks that couldn't be represented by a data 
metric
* Flare returns you an object with all the details of the outcomes of your checks, so you can decide how to
handle them, for example failing your Spark job if the related quality checks are giving errors.
* Flare provides the option to persist the results of your QC Checks to a repository. You can use one of the
built in options or define your own repository for storing to a location of your choice.
* Flare provides the option to persist metrics to a repository. We recommend using the built in Persister for 
ElasticSearch, so that you can easily graph metrics over time with Kibana. Or you can easily use a custom Persister to
track metrics using a datastore of your choosing.

## Comparison to other data quality tools
Flare was inspired by other data quality tools such as [Deequ](https://github.com/awslabs/deequ). We've found other
libraries can make it more challenging to have complex checks, so Flare tries to give greater flexibility and
extensibility by:

* SQD allows metrics-based checks to do comparisons between different metrics and datasets, so that you are able to do
relative comparisons rather than just absolute comparisons.
* Flare allows the definition of custom checks as part of your ChecksSuite. While not as performant as the checks based on
metrics, having the flexibility to include these we feel gives more power and choice.
* All of Flare's storage methods are extensible, giving users the ability to store results and metrics wherever they 
choose

## Getting started
Add the following to your dependencies:
```
libraryDependencies += "com.github.timgent" % "spark-data-quality_2.11" % "@VERSION@"
```
For other build systems like maven, and to check the latest version go to 
https://search.maven.org/artifact/com.github.timgent/spark-data-quality_2.11

You can [find the javadocs here](https://www.javadoc.io/doc/com.github.timgent/spark-data-quality_2.11/latest/index.html#package)

## An example
The rest of the documentation will talk through the main features and how to use them. However some find it easier to
follow an example, so you can 
[find some example code and explanation here](https://github.com/timgent/spark-data-quality/tree/master/src/main/scala/com/github/timgent/data-flare/examples).
