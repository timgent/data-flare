package com.github.timgent.dataflare.metrics

import cats.Show
import com.github.timgent.dataflare.metrics.MetricCalculator.{ComplianceMetricCalculator, DistinctValuesMetricCalculator
  , DistinctnessMetricCalculator, MaxValueMetricCalculator, MinValueMetricCalculator, SizeMetricCalculator, SumValuesMetricCalculator}
import com.github.timgent.dataflare.metrics.MetricValue.NumericMetricValue

/**
  * Describes the metric being calculated
  */
private[dataflare] trait MetricDescriptor {
  type MC <: MetricCalculator

  type MetricType = MC#MetricType

  /**
    * The metricCalculator which contains key logic for calculating a MetricValue for this MetricDescriptor
    * @return the MetricCalculator
    */
  def metricCalculator: MC

  /**
    * A representation of the MetricDescriptor that can be more easily handled for persistence
    * @return the SimpleMetricDescriptor
    */
  def toSimpleMetricDescriptor: SimpleMetricDescriptor

  /**
    * A name for the metric
    * @return
    */
  def metricName: String
}

object MetricDescriptor {

  /**
    * A MetricDescriptor which can have the dataset filtered before the metric is calculated
    */
  trait Filterable {

    /**
      * A filter to apply before calculation of the metric
      * @return the MetricFilter
      */
    def filter: MetricFilter
  }

  /**
    * A metric that calculates the number of rows in your dataset
    * @param filter - filter to be applied before the size is calculated
    */
  case class SizeMetric(filter: MetricFilter = MetricFilter.noFilter) extends MetricDescriptor with Filterable {
    override def metricCalculator: SizeMetricCalculator = SizeMetricCalculator(filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(metricName, Some(filter.filterDescription))
    override def metricName: String = "Size"
    override type MC = SizeMetricCalculator
  }

  /**
    * A metric that calculates the number of rows in your dataset
    * @param filter - filter to be applied before the size is calculated
    */
  case class SumValuesMetric[MV <: NumericMetricValue: MetricValueConstructor](
      onColumn: String,
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: SumValuesMetricCalculator[MV] = SumValuesMetricCalculator[MV](onColumn, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(metricName, Some(filter.filterDescription), onColumn = Some(onColumn))
    override def metricName: String = "SumValues"
    override type MC = SumValuesMetricCalculator[MV]
  }

  /**
    * A metric that calculates the min value of rows in your dataset
    * @param onColumn
    * @param filter filter to be applied before the size is calculated
    * @tparam MV
    */
  case class MinValueMetric[MV <: NumericMetricValue: MetricValueConstructor](
      onColumn: String,
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: MinValueMetricCalculator[MV] = MinValueMetricCalculator[MV](onColumn, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(metricName, Some(filter.filterDescription), onColumn = Some(onColumn))
    override def metricName: String = "MinValue"
    override type MC = MinValueMetricCalculator[MV]
  }

  /**
    *
    * @param onColumn
    * @param filter
    * @tparam MV
    */
  case class MaxValueMetric[MV <: NumericMetricValue: MetricValueConstructor](
       onColumn: String,
       filter: MetricFilter = MetricFilter.noFilter
     ) extends MetricDescriptor
    with Filterable {
    override def metricCalculator: MaxValueMetricCalculator[MV] = MaxValueMetricCalculator[MV](onColumn, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(metricName, Some(filter.filterDescription), onColumn = Some(onColumn))
    override def metricName: String = "MaxValue"
    override type MC = MaxValueMetricCalculator[MV]
  }

  /**
    * A metric that calculates what fraction of rows comply with the given criteria
    * @param complianceFn - the criteria used to check each rows compliance
    * @param filter - a filter to be applied before the compliance fraction is calculated
    */
  case class ComplianceMetric(
      complianceFn: ComplianceFn,
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: ComplianceMetricCalculator =
      ComplianceMetricCalculator(complianceFn, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(
        metricName,
        Some(filter.filterDescription),
        Some(complianceFn.description)
      )
    override def metricName: String = "Compliance"
    override type MC = ComplianceMetricCalculator
  }

  /**
    * A metric that calculates the number of distinct values in a column or across several columns
    * @param onColumns - the columns for which you are counting distinct values
    * @param filter - the filter to be applied before the distinct count is calculated
    */
  case class CountDistinctValuesMetric(
      onColumns: List[String],
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: DistinctValuesMetricCalculator =
      DistinctValuesMetricCalculator(onColumns, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(
        metricName,
        Some(filter.filterDescription),
        onColumns = Some(onColumns)
      )
    override def metricName: String = "DistinctValues"
    override type MC = DistinctValuesMetricCalculator
  }

  /**
    * A metric that calculates how distinct values in a column are (where a result of 1 is that all values are distinct)
    * @param onColumns - the columns for which distinctness is being calculated over
    * @param filter - the filter to be applied before the distinct count is calculated
    */
  case class DistinctnessMetric(
      onColumns: List[String],
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: DistinctnessMetricCalculator =
      DistinctnessMetricCalculator(onColumns, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(
        metricName,
        Some(filter.filterDescription),
        onColumns = Some(onColumns)
      )
    override def metricName: String = "Distinctness"
    override type MC = DistinctnessMetricCalculator
  }
}

/**
  * Representation of a MetricDescriptor which is easy to persist
  */
private[dataflare] case class SimpleMetricDescriptor(
    metricName: String,
    filterDescription: Option[String] = None,
    complianceDescription: Option[String] = None,
    onColumns: Option[List[String]] = None,
    onColumn: Option[String] = None
)

object SimpleMetricDescriptor {
  implicit val showSimpleMetricDescriptor: Show[SimpleMetricDescriptor] = Show.show { descriptor =>
    import descriptor._
    val filterDescriptionStr = filterDescription.map(filterDescription => s", filterDescription=$filterDescription").getOrElse("")
    val complianceDescriptionStr =
      complianceDescription.map(complianceDescription => s", complianceDescription=$complianceDescription").getOrElse("")
    val onColumnsStr = onColumns.map(onColumns => s", onColumns=$onColumns").getOrElse("")
    val onColumnStr = onColumn.map(onColumn => s", onColumn=$onColumn").getOrElse("")
    "metricName=" + metricName + filterDescriptionStr + complianceDescriptionStr + onColumnsStr + onColumnStr
  }
}
