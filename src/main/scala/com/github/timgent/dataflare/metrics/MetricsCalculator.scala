package com.github.timgent.dataflare.metrics

import com.github.timgent.dataflare.FlareError.MetricCalculationError
import com.github.timgent.dataflare.checkssuite.DescribedDs
import com.github.timgent.dataflare.metrics.MetricCalculator.SimpleMetricCalculator
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

private case class DescriptorWithColumnNumber[MC <: MetricCalculator](
    descriptor: MetricDescriptor,
    calculator: MC,
    colNum: Int
)

private[dataflare] object MetricsCalculator {

  def calculateMetrics(
      dds: DescribedDs,
      metricDescriptors: Seq[MetricDescriptor]
  ): Either[MetricCalculationError, Map[MetricDescriptor, MetricValue]] = {
    val distinctDescriptors = metricDescriptors.distinct

    val simpleDescriptors: Seq[DescriptorWithColumnNumber[SimpleMetricCalculator]] =
      distinctDescriptors
        .flatMap { descriptor =>
          descriptor.metricCalculator match {
            case simpleCalculator: SimpleMetricCalculator => Some(descriptor, simpleCalculator)
            case _                                        => None
          }
        }
        .zipWithIndex
        .map {
          case ((descriptor, calculator), colNum) =>
            DescriptorWithColumnNumber(descriptor, calculator, colNum)
        }

    val simpleMetricsTry: Try[Map[MetricDescriptor, SimpleMetricCalculator#MetricType]] = Try {
      simpleDescriptors match {
        case Nil => Map.empty
        case firstAgg :: otherAggs =>
          val dsWithAggregations: DataFrame =
            dds.ds.agg(firstAgg.calculator.aggFunction, otherAggs.map(_.calculator.aggFunction): _*)
          val metricsRow = dsWithAggregations.collect().toSeq.head
          simpleDescriptors.map { descriptor =>
            descriptor.descriptor -> descriptor.calculator.valueFromRow(
              metricsRow,
              descriptor.colNum
            )
          }.toMap
      }
    }
    simpleMetricsTry match {
      case Failure(exception)     => Left(MetricCalculationError(dds, metricDescriptors, Some(exception)))
      case Success(simpleMetrics) => Right(simpleMetrics)
    }
  }
}
