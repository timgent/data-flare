package qualitychecker.constraint

case class RawConstraintResult(status: ConstraintStatus.Value, resultDescription: String) {
  def toConstraintResult(constraint: QCConstraint) = ConstraintResult(status, resultDescription, constraint)
}

case class ConstraintResult(
                           status: ConstraintStatus.Value,
                           resultDescription: String,
                           constraint: QCConstraint
                           )
