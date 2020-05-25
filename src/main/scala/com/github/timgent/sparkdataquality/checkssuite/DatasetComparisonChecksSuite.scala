package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.DatasetComparisonCheck.DatasetPair
import com.github.timgent.sparkdataquality.checks.{CheckResult, DatasetComparisonCheck}
import com.github.timgent.sparkdataquality.checkssuite.ChecksSuiteBase.getOverallCheckResultDescription
import org.apache.spark.sql.Dataset

import scala.concurrent.{ExecutionContext, Future}

trait DatasetComparisonChecksSuite extends ChecksSuiteBase {
  /**
   * The dataset to run the check on
   * @return
   */
  def datasetToCheck: Dataset[_]

  /**
   * The dataset to compare to
   * @return
   */
  def datasetToCompareTo: Dataset[_]
}

object DatasetComparisonChecksSuite {
  def apply(ds: Dataset[_],
            dsToCompare: Dataset[_],
            checkDesc: String,
            checks: Seq[DatasetComparisonCheck],
            checkTags: Map[String, String],
            checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
           ): DatasetComparisonChecksSuite = {
    new DatasetComparisonChecksSuite {
      override def datasetToCheck: Dataset[_] = ds

      override def datasetToCompareTo: Dataset[_] = dsToCompare

      override def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult] = {
        val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(DatasetPair(datasetToCheck, datasetToCompareTo)))
        val overallCheckStatus = checkResultCombiner(checkResults)
        val checkSuiteResult = ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
          checkResults, timestamp, checkTags)
        Future.successful(checkSuiteResult)
      }

      override def checkSuiteDescription: String = checkDesc
    }
  }
}