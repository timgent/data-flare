package com.github.timgent.sparkdataquality.metrics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
 * Defines a filter to be applied before a metric is calculated
 * @param filter - a column containing true or false depending if the row should be included in the metrics calculation
 * @param filterDescription - a readable description of the filter (used for persistence)
 */
case class MetricFilter(filter: Column, filterDescription: String)

object MetricFilter {
  /**
   * Don't apply any filter
   * @return A MetricFilter that doesn't filter records at all
   */
  def noFilter = MetricFilter(lit(true), "no filter")
}