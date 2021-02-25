package com.github.timgent.dataflare.metrics

import com.github.timgent.dataflare.metrics.MetricValue.{DoubleMetric, LongMetric, NumericMetricValue, OptNumericMetricValue}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row}

private[dataflare] sealed trait MetricCalculator {
  type MetricType <: MetricValue
}

private[dataflare] object MetricCalculator {

  /**
    * MetricCalculator that calculates metrics based on a simple aggregation function on the whole dataset
    */
  sealed trait SimpleMetricCalculator extends MetricCalculator {

    /**
      *
      * @return the value that should be used for the metric if there is no data in the dataset
      */
    protected def metricValueForEmptyDs: MetricType#T = metricValueConstructor.zero

    protected def metricValueConstructor: MetricValueConstructor[MetricType]

    def aggFunction: Column

    def valueFromRow(row: Row, index: Int): MetricType = {
      val metricValue: Option[MetricType#T] =
        Option(row.getAs[MetricType#T](index)) // empty DS would return null from row.getAs, hence use of option here
      metricValue match {
        case Some(value) => metricValueConstructor.apply(value)
        case None        => metricValueConstructor.apply(metricValueForEmptyDs)
      }
    }

    def filter: MetricFilter
  }

  /**
    * MetricCalculator that calculates metrics based on a simple aggregation function on the whole dataset,
    * tailored to metrics that can return None values
    */
  sealed trait SimpleOptMetricCalculator extends SimpleMetricCalculator {
    override type MetricType <: OptNumericMetricValue

    override def valueFromRow(row: Row, index: Int): MetricType = {
      // Here we must get underlying type U. If we used MetricType#T, row.getAs
      // wouldn't respect this at runtime, returning the underlying type instead,
      // and it would fail to construct the metric
      val metricValue: Option[MetricType#U] =
        Option(row.getAs[MetricType#U](index)) // empty DS would return null from row.getAs, hence use of option here
      metricValue match {
        case Some(value) => metricValueConstructor.apply(Some(value))
        case None        => metricValueConstructor.apply(metricValueForEmptyDs)
      }
    }
  }

  sealed trait DoubleMetricCalculator extends SimpleMetricCalculator {
    override type MetricType = DoubleMetric

    override protected def metricValueConstructor: MetricValueConstructor[DoubleMetric] = MetricValueConstructor.DoubleMetricConstructor
  }

  sealed trait LongMetricCalculator extends SimpleMetricCalculator {
    override type MetricType = LongMetric

    override protected def metricValueConstructor: MetricValueConstructor[LongMetric] = MetricValueConstructor.LongMetricConstructor
  }

  case class SizeMetricCalculator(filter: MetricFilter) extends LongMetricCalculator {
    override def aggFunction: Column = {
      sum(when(filter.filter, 1).otherwise(0))
    }
  }

  case class ComplianceMetricCalculator(complianceFn: ComplianceFn, filter: MetricFilter) extends DoubleMetricCalculator {
    override def metricValueForEmptyDs: Double = 1.0

    override def aggFunction: Column = {
      val numberOfCompliantRows = sum(when(filter.filter and complianceFn.definition, 1).otherwise(0))
      val totalRows = sum(when(filter.filter, 1).otherwise(0))
      numberOfCompliantRows / totalRows
    }
  }

  case class SumValuesMetricCalculator[MV <: NumericMetricValue: MetricValueConstructor](onColumn: String, filter: MetricFilter)
      extends SimpleMetricCalculator {
    override protected def metricValueConstructor: MetricValueConstructor[MV] = implicitly[MetricValueConstructor[MV]]

    override type MetricType = MV

    override def aggFunction: Column = sum(when(filter.filter, col(onColumn)).otherwise(0))
  }

  case class MinValueMetricCalculator[MV <: OptNumericMetricValue: MetricValueConstructor](onColumn: String, filter: MetricFilter)
      extends SimpleOptMetricCalculator {
    override protected def metricValueConstructor: MetricValueConstructor[MV] = implicitly[MetricValueConstructor[MV]]

    override type MetricType = MV

    // .get here is safe as maxValue will always be a Some. Required as Spark can't handle the optional value
    override def aggFunction: Column = min(when(filter.filter, col(onColumn)).otherwise(metricValueConstructor.maxValue.get))
  }

  case class MaxValueMetricCalculator[MV <: OptNumericMetricValue: MetricValueConstructor](onColumn: String, filter: MetricFilter)
      extends SimpleOptMetricCalculator {
    override protected def metricValueConstructor: MetricValueConstructor[MV] = implicitly[MetricValueConstructor[MV]]

    override type MetricType = MV

    // .get here is safe as minValue will always be a Some. Required as Spark can't handle the optional value
    override def aggFunction: Column = max(when(filter.filter, col(onColumn)).otherwise(metricValueConstructor.minValue.get))
  }

  case class DistinctValuesMetricCalculator(onColumns: List[String], filter: MetricFilter) extends LongMetricCalculator {
    override def aggFunction: Column = {
      val countDistinctCols: List[Column] =
        onColumns.map(onColumn => when(not(filter.filter), null).otherwise(col(onColumn)))
      countDistinct(
        countDistinctCols.head,
        countDistinctCols.tail: _*
      ) // TODO: Handle empty col list case and bad filter case
    }
  }

  case class DistinctnessMetricCalculator(onColumns: List[String], filter: MetricFilter) extends DoubleMetricCalculator {
    override def metricValueForEmptyDs: Double = 1.0

    override def aggFunction: Column = {
      val countDistinctCols: List[Column] =
        onColumns.map(onColumn => when(not(filter.filter), null).otherwise(col(onColumn)))
      val distinctCount = countDistinct(
        countDistinctCols.head,
        countDistinctCols.tail: _*
      ) // TODO: Handle empty col list case and bad filter case
      val size = sum(when(filter.filter, 1).otherwise(0))
      distinctCount / size
    }
  }

}
