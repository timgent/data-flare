---
id: metricsintro
title: Introduction to metrics and metric based checks
sidebar_label: Introduction to metrics and metric based checks
---
## Why metric based checks?
We've implemented data quality checks based on metrics for a few reasons:

* Efficiency - all metrics on a dataset that are required for any check will be computed in one pass over the data.
This means that these checks are much more efficient than custom checks.
* Tracking - writing data quality checks for your data will go a long way towards ensuring your data quality is high.
However, being able to track metrics and time and easily graph them (for example with ElasticSearch and Kibana) means
a human can more easily spot issues with the trends you see there, and can also help to identify where the causes for
data quality issues may be coming from.

There are 2 types of checks that use metrics. Both of them enable you to define a metric you want to check, and then
perform some validation on them. You'll also find a number of helper methods available to help you perform common
metric checks more concisely.