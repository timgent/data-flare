package com.github.timgent.dataflare.checks

import com.github.timgent.dataflare.FlareError.ArbCheckError
import com.github.timgent.dataflare.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.dataflare.checks.QCCheck.DualDsQCCheck
import com.github.timgent.dataflare.checkssuite.DescribedDsPair
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}

/**
  * Check for comparing a pair of datasets
  */
trait ArbDualDsCheck extends DualDsQCCheck {
  def description: CheckDescription

  override def qcType: QcType = QcType.ArbDualDsCheck

  def applyCheck(dsPair: DescribedDsPair): CheckResult
}

object ArbDualDsCheck {

  case class DatasetPair(ds: Dataset[_], dsToCompare: Dataset[_])

  def apply(
      checkDescription: String
  )(check: DatasetPair => RawCheckResult): ArbDualDsCheck = {
    new ArbDualDsCheck {
      override def description: SimpleCheckDescription = SimpleCheckDescription(checkDescription)

      override def applyCheck(ddsPair: DescribedDsPair): CheckResult = {
        val maybeRawCheckResult = Try(check(ddsPair.rawDatasetPair))
        maybeRawCheckResult match {
          case Failure(exception) =>
            CheckResult(
              qcType,
              CheckStatus.Error,
              "Check failed due to unexpected exception during evaluation",
              description,
              Some(ddsPair.datasourceDescription),
              errors = Seq(ArbCheckError(Some(ddsPair.datasourceDescription), description, Some(exception)))
            )
          case Success(rawCheckResult) => rawCheckResult.withDescription(qcType, description, Some(ddsPair.datasourceDescription))
        }
      }
    }
  }

  /**
    * Checks if schemas of 2 datasets exactly match
    */
  val dsSchemasMatch = ArbDualDsCheck("Dataset schemas match") { dsPair =>
    val df1Fields = dsPair.ds.schema.fields.sortBy(_.name).toList
    val df2Fields = dsPair.dsToCompare.schema.fields.sortBy(_.name).toList
    if (df1Fields == df2Fields) {
      RawCheckResult(CheckStatus.Success, "Dataset schemas successfully matched")
    } else {
      val colsInDf1Not2 = df1Fields.map(_.name).diff(df2Fields.map(_.name))
      val colsInDf2Not1 = df2Fields.map(_.name).diff(df1Fields.map(_.name))
      val commonCols = df1Fields.map(_.name).intersect(df2Fields.map(_.name))
      val df1CommonFields = df1Fields.filter(field => commonCols.contains(field.name))
      val df2CommonFields = df2Fields.filter(field => commonCols.contains(field.name))
      val zippedCommonFields = df1CommonFields.zip(df2CommonFields)
      val fieldsWithTypeErrors = zippedCommonFields.filter { case (df1Field, df2Field) => df1Field.dataType != df2Field.dataType }
      val errors =
        colsInDf1Not2.map(c => s"Column $c was present only in ds") ++
          colsInDf2Not1.map(c => s"Column $c was present only in dsToCompare") ++
          fieldsWithTypeErrors.map { case (f1, f2) => s"Column ${f1.name} types did not match (${f1.dataType} vs ${f2.dataType})" }
      val errMsg = "Dataset schemas did not match: " + errors.mkString(". ")
      RawCheckResult(CheckStatus.Error, errMsg)
    }
  }
}
