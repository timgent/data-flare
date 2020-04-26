package qualitychecker.constraint

import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.functions.sum
import qualitychecker.constraint.QCConstraint.DatasetComparisonConstraint.DatasetPair
import qualitychecker.thresholds.AbsoluteThreshold
import qualitychecker.{CheckStatus, DeequCheck}

sealed trait QCConstraint {
  def description: String
}

object QCConstraint {

  trait SingleDatasetConstraint extends QCConstraint {
    def description: String

    def applyConstraint(ds: Dataset[_]): ConstraintResult
  }

  object SingleDatasetConstraint {
    def apply(constraintDescription: String)(constraint: Dataset[_] => RawConstraintResult): SingleDatasetConstraint = {
      new SingleDatasetConstraint {
        override def description: String = constraintDescription

        override def applyConstraint(ds: Dataset[_]): ConstraintResult =
          constraint(ds).toConstraintResult(this)
      }
    }

    def sumOfValuesConstraint[T: Encoder](columnName: String, threshold: AbsoluteThreshold[T]): SingleDatasetConstraint = {
      SingleDatasetConstraint(s"Constraining sum of column $columnName to be within threshold $threshold") { ds =>
        val sumOfCol = ds.agg(sum(columnName)).as[T].first()
        val withinThreshold = threshold.isWithinThreshold(sumOfCol)
        if (withinThreshold)
          RawConstraintResult(CheckStatus.Success, s"Sum of column $columnName was $sumOfCol, which was within the threshold $threshold")
        else
          RawConstraintResult(CheckStatus.Error, s"Sum of column $columnName was $sumOfCol, which was outside the threshold $threshold")
      }
    }
  }

  trait DatasetComparisonConstraint extends QCConstraint {
    def description: String

    def applyConstraint(dsPair: DatasetPair): ConstraintResult
  }


  object DatasetComparisonConstraint {

    case class DatasetPair(ds: Dataset[_], dsToCompare: Dataset[_])

    def apply(constraintDescription: String)(constraint: DatasetPair => RawConstraintResult): DatasetComparisonConstraint = {
      new DatasetComparisonConstraint {
        override def description: String = constraintDescription

        override def applyConstraint(dsPair: DatasetPair): ConstraintResult = {
          constraint(dsPair).toConstraintResult(this)
        }
      }
    }
  }

  trait ArbitraryConstraint extends QCConstraint {
    def description: String

    def applyConstraint: ConstraintResult
  }

  object ArbitraryConstraint {
    def apply(constraintDescription: String)(constraint: => RawConstraintResult) = new ArbitraryConstraint {
      override def description: String = constraintDescription

      override def applyConstraint: ConstraintResult = constraint.toConstraintResult(this)
    }
  }

  case class DeequQCConstraint(check: DeequCheck) extends QCConstraint {
    override def description: String = check.description
  }

}
