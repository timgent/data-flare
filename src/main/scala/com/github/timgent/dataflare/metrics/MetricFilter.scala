package com.github.timgent.dataflare.metrics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, col}

/**
  * Defines a filter to be applied before a metric is calculated
  * @param filter - a column containing true or false depending if the row should be included in the metrics calculation
  * @param filterDescription - a readable description of the filter (used for persistence)
  */
case class MetricFilter(filter: Column, filterDescription: String)

object MetricFilter {

  def apply(filter: Column): MetricFilter = MetricFilter(filter, filter.toString)
  def apply(filter: String): MetricFilter = MetricFilter(col(filter), filter)

  /**
    * Don't apply any filter
    * @return A MetricFilter that doesn't filter records at all
    */
  def noFilter = MetricFilter(lit(true), "no filter")
}
