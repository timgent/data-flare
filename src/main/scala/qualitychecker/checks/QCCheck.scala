package qualitychecker.checks

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Dataset, Encoder}
import qualitychecker.DeequCheck
import qualitychecker.checks.QCCheck.DatasetComparisonCheck.DatasetPair
import qualitychecker.thresholds.AbsoluteThreshold

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

        override def applyCheck(ds: Dataset[_]): CheckResult =
          check(ds).toCheckResult(this)
      }
    }

    def sumOfValuesCheck[T: Encoder](columnName: String, threshold: AbsoluteThreshold[T]): SingleDatasetCheck = {
      SingleDatasetCheck(s"Constraining sum of column $columnName to be within threshold $threshold") { ds =>
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
          check(dsPair).toCheckResult(this)
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

      override def applyCheck: CheckResult = check.toCheckResult(this)
    }
  }

  case class DeequQCCheck(check: DeequCheck) extends QCCheck {
    override def description: String = check.description
  }

}

object CheckStatus extends Enumeration {
  val Success, Warning, Error = Value
}