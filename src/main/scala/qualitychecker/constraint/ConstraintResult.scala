package qualitychecker.constraint

import qualitychecker.CheckStatus

case class RawConstraintResult(status: CheckStatus.Value, resultDescription: String) {
  def toConstraintResult(constraint: QCConstraint) = ConstraintResult(status, resultDescription, constraint)
}

case class ConstraintResult(
                           status: CheckStatus.Value,
                           resultDescription: String,
                           constraint: QCConstraint
                           )
