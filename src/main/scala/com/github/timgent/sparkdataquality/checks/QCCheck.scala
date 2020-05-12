package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.checks.QCCheck.DatasetComparisonCheck.DatasetPair
import com.github.timgent.sparkdataquality.sparkdataquality.DeequCheck
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import enumeratum._
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Dataset, Encoder}

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