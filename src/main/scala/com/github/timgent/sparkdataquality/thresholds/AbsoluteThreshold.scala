package com.github.timgent.sparkdataquality.thresholds

case class AbsoluteThreshold[T: Ordering](lowerBound: Option[T], upperBound: Option[T]) {
  def isWithinThreshold(itemToCompare: T): Boolean = {
    val ordering = implicitly[Ordering[T]]
    val isWithinLowerBound = lowerBound.forall(ordering.compare(itemToCompare, _) >= 0)
    val isWithinUpperBound = upperBound.forall(ordering.compare(itemToCompare, _) <= 0)
    isWithinLowerBound && isWithinUpperBound
  }

  override def toString: String = (lowerBound, upperBound) match {
    case (None, None) => "no threshold set"
    case (Some(lowerBound), None) => s"great than $lowerBound"
    case (None, Some(upperBound)) => s"less than $upperBound"
    case (Some(lowerBound), Some(upperBound)) => s"between $lowerBound and $upperBound"
  }
}