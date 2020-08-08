package com.github.timgent.dataflare.metrics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/**
  * Describes a compliance check for a dataset
  * @param definition - a column definition which should return true or false. true if the dataset row meets the
  *                   compliance check you want, otherwise false.
  * @param description - a description of the compliance function which will be used when persisting corresponding
  *                    metrics
  */
case class ComplianceFn(definition: Column, description: String)

object ComplianceFn {
  def apply(filter: Column): ComplianceFn = ComplianceFn(filter, filter.toString)
  def apply(filter: String): ComplianceFn = ComplianceFn(col(filter), filter)
}