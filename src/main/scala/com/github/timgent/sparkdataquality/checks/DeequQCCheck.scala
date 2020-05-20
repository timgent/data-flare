package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.sparkdataquality.DeequCheck

case class DeequQCCheck(check: DeequCheck) extends QCCheck {
  override def description: String = check.description
}
