package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.{VerificationRunBuilder, VerificationSuite}
import com.github.timgent.sparkdataquality.checks.QCCheck.DeequQCCheck
import com.github.timgent.sparkdataquality.deequ.DeequHelpers.VerificationResultToQualityCheckResult
import com.github.timgent.sparkdataquality.sparkdataquality.DeequMetricsRepository
import org.apache.spark.sql.Dataset

case class DeequChecksSuite(dataset: Dataset[_], checkSuiteDescription: String, deequChecks: Seq[DeequQCCheck],
                            checkTags: Map[String, String]
                           )(implicit deequMetricsRepository: DeequMetricsRepository)
  extends ChecksSuite {
  override def qcType: QcType = QcType.DeequQualityCheck

  override def run(timestamp: Instant): ChecksSuiteResult = {
    val verificationSuite: VerificationRunBuilder = VerificationSuite()
      .onData(dataset.toDF)
      .useRepository(deequMetricsRepository)
      .saveOrAppendResult(ResultKey(timestamp.toEpochMilli))

    verificationSuite.addChecks(deequChecks.map(_.check))
      .run()
      .toCheckSuiteResult(checkSuiteDescription, timestamp, checkTags)
  }
}
