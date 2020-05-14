package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.CheckResult
import com.github.timgent.sparkdataquality.checks.QCCheck.MetricsBasedCheck
import com.github.timgent.sparkdataquality.metrics.{MetricDescriptor, MetricValue, MetricsCalculator}
import org.apache.spark.sql.Dataset

case class MetricsBasedChecksSuite(dataset: Dataset[_],
                                   checks: Seq[MetricsBasedCheck[_, _]],
                                   checkSuiteDescription: String,
                                   tags: Map[String, String],
                                   checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
                                  ) extends ChecksSuite {
  override def run(timestamp: Instant): ChecksSuiteResult = {
    val metricDescriptors: Seq[MetricDescriptor] = checks.map(_.metricDescriptor).distinct
    val calculatedMetrics: Map[MetricDescriptor, MetricValue] = MetricsCalculator.calculateMetrics(dataset, metricDescriptors)
    val checkResults: Seq[CheckResult] = checks.map(_.applyCheckOnMetrics(calculatedMetrics))
    val overallCheckResult = checkResultCombiner(checkResults)
    ChecksSuiteResult(
      overallCheckResult,
      checkSuiteDescription,
      ChecksSuite.getOverallCheckResultDescription(checkResults),
      checkResults,
      timestamp,
      qcType,
      tags
    )
  }

  override def qcType: QcType = QcType.MetricsBasedQualityCheck
}

//object MetricsBasedChecksSuite {
//  def apply(dataset: Dataset[_], checks: Seq[MetricsBasedCheck[_]], checkSuiteDescription: String): MetricsBasedChecksSuite = {
//    ???
//  }
//}