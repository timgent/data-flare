package com.github.timgent.sparkdataquality.metrics

import org.apache.spark.sql.Column

case class ComplianceFn(definition: Column, description: String)
