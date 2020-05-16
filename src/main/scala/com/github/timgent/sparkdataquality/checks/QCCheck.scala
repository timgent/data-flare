package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.QCCheck.DatasetComparisonCheck.DatasetPair
import com.github.timgent.sparkdataquality.metrics.MetricCalculator.{SimpleMetricCalculator, SizeMetricCalculator}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.SizeMetricDescriptor
import com.github.timgent.sparkdataquality.metrics.{MetricCalculator, MetricDescriptor, MetricFilter, MetricValue}
import com.github.timgent.sparkdataquality.sparkdataquality.DeequCheck
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import enumeratum._
import javassist.bytecode.stackmap.TypeTag
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

sealed trait QCCheck {
  def description: String
}

object QCCheck {

  trait SingleDatasetCheck extends QCCheck {
    def description: String

    def applyCheck(ds: Dataset[_]): CheckResult
  }

  object SingleDatasetCheck {
    def apply(checkDescription: String)(check: Dataset[_] => RawCheckResult): SingleDatasetCheck = {
      new SingleDatasetCheck {
        override def description: String = checkDescription

        override def applyCheck(ds: Dataset[_]): CheckResult = check(ds).withDescription(checkDescription)
      }
    }

    def sumOfValuesCheck[T: Encoder](columnName: String, threshold: AbsoluteThreshold[T]): SingleDatasetCheck = {
      val checkDescription = s"Constraining sum of column $columnName to be within threshold $threshold"
      SingleDatasetCheck(checkDescription) { ds =>
        val sumOfCol = ds.agg(sum(columnName)).as[T].first()
        val withinThreshold = threshold.isWithinThreshold(sumOfCol)
        if (withinThreshold)
          RawCheckResult(CheckStatus.Success, s"Sum of column $columnName was $sumOfCol, which was within the threshold $threshold")
        else
          RawCheckResult(CheckStatus.Error, s"Sum of column $columnName was $sumOfCol, which was outside the threshold $threshold")
      }
    }
  }

  trait DatasetComparisonCheck extends QCCheck {
    def description: String

    def applyCheck(dsPair: DatasetPair): CheckResult
  }


  object DatasetComparisonCheck {

    case class DatasetPair(ds: Dataset[_], dsToCompare: Dataset[_])

    def apply(checkDescription: String)(check: DatasetPair => RawCheckResult): DatasetComparisonCheck = {
      new DatasetComparisonCheck {
        override def description: String = checkDescription

        override def applyCheck(dsPair: DatasetPair): CheckResult = {
          check(dsPair).withDescription(checkDescription)
        }
      }
    }
  }

  trait ArbitraryCheck extends QCCheck {
    def description: String

    def applyCheck: CheckResult
  }

  object ArbitraryCheck {
    def apply(checkDescription: String)(check: => RawCheckResult) = new ArbitraryCheck {
      override def description: String = checkDescription

      override def applyCheck: CheckResult = check.withDescription(checkDescription)
    }
  }

  trait MetricsBasedCheck[MV <: MetricValue, MC <: MetricCalculator] extends QCCheck {
    def metricDescriptor: MetricDescriptor

    protected def applyCheck(metric: MV): CheckResult

    // typeTag required here to enable match of metric on type MV. Without class tag this type check would be fruitless
    final def applyCheckOnMetrics(metrics: Map[MetricDescriptor, MetricValue])(implicit classTag: ClassTag[MV]): CheckResult = {
      val metricOfInterestOpt: Option[MetricValue] =
        metrics.get(metricDescriptor).map(metricValue => metricValue)
      metricOfInterestOpt match {
        case Some(metric) =>
          metric match { // TODO: Look into heterogenous maps to avoid this type test - https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-1.2.4#heterogenous-maps
            case metric: MV => applyCheck(metric)
            case _ => CheckResult(CheckStatus.Error, "Found metric of the wrong type for this check. Please report this error - this should not occur", description)
          }
        case None => CheckResult(CheckStatus.Error, "Failed to find corresponding metric for this check. Please report this error - this should not occur", description)
      }
    }
  }

  object MetricsBasedCheck {
    case class SizeCheck(threshold: AbsoluteThreshold[Long], filter: MetricFilter) extends MetricsBasedCheck[MetricValue.LongMetric, SizeMetricCalculator] {
      override protected def applyCheck(metric: MetricValue.LongMetric): CheckResult = {
        val sizeIsWithinThreshold = threshold.isWithinThreshold(metric.value)
        if (sizeIsWithinThreshold) {
          CheckResult(CheckStatus.Success, s"Size of ${metric.value} was within the range $threshold", description)
        } else {
          CheckResult(CheckStatus.Error, s"Size of ${metric.value} was outside the range $threshold", description)
        }
      }

      override def description: String = s"SizeCheck with filter: ${filter.filterDescription}"

      override def metricDescriptor: MetricDescriptor = SizeMetricDescriptor(filter)
    }
  }

  case class DeequQCCheck(check: DeequCheck) extends QCCheck {
    override def description: String = check.description
  }

}

sealed trait CheckStatus extends EnumEntry

object CheckStatus extends Enum[CheckStatus] {
  val values = findValues
  case object Success extends CheckStatus
  case object Warning extends CheckStatus
  case object Error extends CheckStatus
}