package com.github.timgent.sparkdataquality.checks

import com.github.timgent.sparkdataquality.sparkdataquality.DeequCheck

/**
  * Light wrapper for a deequ check
  * @param check
  */
case class DeequQCCheck(check: DeequCheck) extends QCCheck {
  override def description: String = check.description

  override def qcType: QcType = QcType.DeequQualityCheck
}
