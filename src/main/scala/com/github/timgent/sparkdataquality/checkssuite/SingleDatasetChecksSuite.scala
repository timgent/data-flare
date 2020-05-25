package com.github.timgent.sparkdataquality.checkssuite

import java.time.Instant

import com.github.timgent.sparkdataquality.checks.{CheckResult, SingleDatasetCheck}
import com.github.timgent.sparkdataquality.checkssuite.ChecksSuiteBase.getOverallCheckResultDescription
import org.apache.spark.sql.Dataset

import scala.concurrent.{ExecutionContext, Future}

object SingleDatasetChecksSuite {
  def apply(describedDataset: DescribedDataset,
            checkDesc: String,
            checks: Seq[SingleDatasetCheck],
            checkTags: Map[String, String],
            checkResultCombiner: Seq[CheckResult] => CheckSuiteStatus = ChecksSuiteResultStatusCalculator.getWorstCheckStatus
           ): SingleDatasetChecksSuite = {
    new SingleDatasetChecksSuite {
      def run(timestamp: Instant)(implicit ec: ExecutionContext): Future[ChecksSuiteResult] = {
        val checkResults: Seq[CheckResult] = checks.map(_.applyCheck(dataset).withDatasourceDescription(describedDataset.description))
        val overallCheckStatus = checkResultCombiner(checkResults)
        val checksSuiteResult = ChecksSuiteResult(overallCheckStatus, checkSuiteDescription, getOverallCheckResultDescription(checkResults),
          checkResults, timestamp, checkTags)
        Future.successful(checksSuiteResult)
      }

      override def dataset: Dataset[_] = describedDataset.ds

      override def checkSuiteDescription: String = checkDesc
    }
  }
}

trait SingleDatasetChecksSuite extends ChecksSuiteBase {
  /**
   * The dataset for checks to be done on
   * @return
   */
  def dataset: Dataset[_]
}