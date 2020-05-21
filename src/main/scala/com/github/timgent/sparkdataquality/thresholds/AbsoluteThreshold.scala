package com.github.timgent.sparkdataquality.thresholds

/**
 * Used to define a threshold which can be used in various checks
 * @param lowerBound - The minimum acceptable value. If None then there is no minimum acceptable value
 * @param upperBound - The maximum acceptable value. If None then there is no maximum acceptable value
 * @tparam T - The type of value the check is performed on. There must be an implicit Ordering in scope for this type
 */
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