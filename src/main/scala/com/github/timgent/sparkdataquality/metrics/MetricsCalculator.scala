package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricCalculator.SimpleMetricCalculator
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SizeMetricDescriptor
import org.apache.spark.sql.{DataFrame, Dataset}

private case class DescriptorWithColumnNumber[MC <: MetricCalculator](descriptor: MetricDescriptor, calculator: MC, colNum: Int)

object MetricsCalculator {

  def calculateMetrics(ds: Dataset[_],
                       metricDescriptors: Seq[MetricDescriptor]): Map[MetricDescriptor, MetricValue] = {
    val distinctDescriptors = metricDescriptors.distinct

    val simpleDescriptors: Seq[DescriptorWithColumnNumber[SimpleMetricCalculator]] = distinctDescriptors.flatMap{ descriptor =>
      descriptor.metricCalculator match {
        case simpleCalculator: SimpleMetricCalculator => Some(descriptor, simpleCalculator)
        case _ => None
      }
    }.zipWithIndex.map{case ((descriptor, calculator), colNum) => DescriptorWithColumnNumber(descriptor, calculator, colNum)} // TODO: Not sure I need the index now?

    val simpleMetrics: Map[MetricDescriptor, SimpleMetricCalculator#MetricType] = simpleDescriptors match {
      case Nil => Map.empty
      case firstAgg :: otherAggs =>
        val dsWithAggregations: DataFrame =
          ds.agg(firstAgg.calculator.aggFunction, otherAggs.map(_.calculator.aggFunction): _*)
        val metricsRow = dsWithAggregations.collect().toSeq.head
        simpleDescriptors.map{descriptor =>
          descriptor.descriptor -> descriptor.calculator.valueFromRow(metricsRow, descriptor.colNum)
        }.toMap
    }
    simpleMetrics
  }
}
