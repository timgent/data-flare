package com.github.timgent.sparkdataquality.metrics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

case class MetricFilter(filter: Column, filterDescription: String)

object MetricFilter {
  def noFilter = MetricFilter(lit(true), "no filter")
}