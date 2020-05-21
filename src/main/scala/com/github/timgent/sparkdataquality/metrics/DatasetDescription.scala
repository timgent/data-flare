package com.github.timgent.sparkdataquality.metrics

/**
 * Wrapper for a string describing a Dataset
 * @param value - the description of the Dataset
 */
case class DatasetDescription(value: String) {
  override def toString: String = value
}
