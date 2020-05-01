package com.github.sparkdataquality.thresholds

case class AbsoluteThreshold[T: Ordering](lowerBound: Option[T], upperBound: Option[T]) {
  def isWithinThreshold(itemToCompare: T): Boolean = {
    val ordering = implicitly[Ordering[T]]
    val isWithinLowerBound = lowerBound.forall(ordering.compare(itemToCompare, _) >= 0)
    val isWithinUpperBound = upperBound.forall(ordering.compare(itemToCompare, _) <= 0)
    isWithinLowerBound && isWithinUpperBound
  }
}