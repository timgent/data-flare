package com.github.timgent.sparkdataquality.deequ

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{
  AnalysisResult,
  MetricsRepository,
  MetricsRepositoryMultipleResultsLoader,
  ResultKey
}

/**
  * Use this if you don't want to store metrics from deequ
  */
class DeequNullMetricsRepository extends MetricsRepository {
  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {}

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = None

  override def load(): MetricsRepositoryMultipleResultsLoader =
    new MetricsRepositoryMultipleResultsLoader {
      override def withTagValues(
          tagValues: Map[String, String]
      ): MetricsRepositoryMultipleResultsLoader = this

      override def forAnalyzers(
          analyzers: Seq[Analyzer[_, Metric[_]]]
      ): MetricsRepositoryMultipleResultsLoader = this

      override def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = this

      override def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = this

      override def get(): Seq[AnalysisResult] = Seq.empty
    }
}
