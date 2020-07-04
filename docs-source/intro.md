---
id: intro
title: Introduction to Spark Data Quality
sidebar_label: Introduction
---

Spark Data Quality (SDQ) is a data quality library built on top of [Apache Spark](https://spark.apache.org/), and enables
you assure the data quality of large scale datasets, both by providing fast feedback on the quality of your data, and
by enabling you to easily store and visualize key metrics for your data and track them over time.

## How does it work?
* You define checks in Scala code, giving you complete flexibility with the checks you want to define.
** Checks can be based on metrics associated with the data, such as the size of a dataset, level of compliance with a
particular condition, etc. SDQ runs these checks efficiently, using a single pass over the data to calculate all
metrics.
** Checks can be completely arbitrary, enabling you to do more complex checks that couldn't be represented by a data 
metric
* SDQ returns you an object with all the details of the outcomes of your checks, so you can decide how to
handle them, for example failing your Spark job if the related quality checks are giving errors.
* SDQ provides the option to persist the results of your QC Checks to a repository. You can use one of the
built in options or define your own repository for storing to a location of your choice.
* SDQ provides the option to persist metrics to a repository. We recommend using the built in Persister for 
ElasticSearch, so that you can easily graph metrics over time with Kibana. Or you can easily use a custom Persister to
track metrics using a datastore of your choosing.
* SDQ also supports performing [Deequ](https://github.com/awslabs/deequ) checks, which gives a greater range of 
built-in checks at present. However, please be aware metrics related to Deequ are persisted separately to other SDQ 
metrics, and currently Deequ only supports persisting of Deequ metrics as a JSON blob in a file. For this reason we 
recommend using SDQ checks where possible.

## Getting started
Add the following to your dependencies:
```
libraryDependencies += "com.github.timgent" % "spark-data-quality_2.11" % "x.x.x"
```
For other build systems like maven, and to check the latest version go to 
https://search.maven.org/artifact/com.github.timgent/spark-data-quality_2.11

You can [find the javadocs here](https://www.javadoc.io/doc/com.github.timgent/spark-data-quality_2.11/latest/index.html#package)

## An example
The rest of the documentation will talk through the main features and how to use them. However some find it easier to
follow an example, so you can [find some example code and explanation here](src/main/scala/com/github/timgent/sparkdataquality/examples).
