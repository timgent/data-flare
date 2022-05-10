package com.github.timgent.dataflare.checks

import cats.Foldable
import com.github.timgent.dataflare.FlareError.ArbCheckError
import com.github.timgent.dataflare.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.dataflare.checks.QCCheck.DualDsQCCheck
import com.github.timgent.dataflare.checkssuite.DescribedDsPair
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

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

  private def zipWithIndex[T](rdd: RDD[T]): RDD[(Long, T)] = rdd.zipWithIndex.map { case (row, i) => (i, row) }

  /**
    * Check that dfs match exactly regardless of content
    */
  val dfsMatchUnordered: ArbDualDsCheck = ArbDualDsCheck("Unordered dataset content matches") { dsPair =>
    val schemaMatchCheckResult = doSchemasMatch(dsPair)
    schemaMatchCheckResult match {
      case Left(errMsg) => RawCheckResult(CheckStatus.Error, errMsg)
      case Right(schema) =>
        val cols: Array[String] = schema.fields.map(f => f.name)
        val renamedCols = cols.map(c => s"${c}_to_compare")
        val colsWithCount: Array[String] = "count" +: cols
        val renamedColsWithCount = "count_to_compare" +: renamedCols
        val dsRowCounts = dsPair.ds.groupBy(cols.map(col): _*).agg(count("*").as("count"))
        def renameCols(ds: Dataset[_]): DataFrame = {
          cols.zip(renamedCols).foldLeft(ds.toDF) { case (ds, (c, cRenamed)) => ds.withColumnRenamed(c, cRenamed) }
        }
        val dsToCompareRowCounts =
          renameCols(dsPair.dsToCompare).groupBy(renamedCols.map(col): _*).agg(count("*").as("count_to_compare"))
        val joinExpr = colsWithCount.zip(renamedColsWithCount).foldLeft(lit(true)) {
          case (joinExpr, (c, cRenamed)) => joinExpr && col(c) === col(cRenamed)
        }
        val joined = dsRowCounts.join(dsToCompareRowCounts, joinExpr, "fullouter")
        val diffRows =
          joined.filter(col(cols.head).isNull || col(renamedCols.head).isNull).orderBy(joined.columns.head, joined.columns.tail: _*)
        val firstBadRow = diffRows.limit(1).collect.toList.headOption
        firstBadRow match {
          case Some(badRow) =>
            if (Option(badRow.getAs[Long]("count")).isEmpty) {
              val mismatchedRowInDsToCompare: Map[String, Any] = badRow.getValuesMap(renamedCols)
              val mismatchedRowWithOriginalColNames = mismatchedRowInDsToCompare.map {
                case (c, v) => (c.substring(0, c.length - "_to_compare".length), v)
              }
              val mismatchedRowCount = badRow.getAs[Long]("count_to_compare")
              RawCheckResult(
                CheckStatus.Error,
                s"Datasets did not match: First mismatch was in dsToCompare where ${mismatchedRowCount} row(s) had " +
                  s"content: ${prettyValuesMap(mismatchedRowWithOriginalColNames)}"
              )
            } else {
              val mismatchedRowInDs = badRow.getValuesMap(cols)
              val mismatchedRowCount = badRow.getAs[Long]("count")
              RawCheckResult(
                CheckStatus.Error,
                s"Datasets did not match: First mismatch was in ds where ${mismatchedRowCount} row(s) had " +
                  s"content: ${prettyValuesMap(mismatchedRowInDs)}"
              )
            }
          case None => RawCheckResult(CheckStatus.Success, "Datasets matched")
        }
    }
  }

  /**
    * Checks that dfs match exactly and are in the same order
    */
  val dfsMatchOrdered: ArbDualDsCheck = ArbDualDsCheck("Ordered dataset content matches") { dsPair =>
    import cats.implicits._
    val schemaMatchCheckResult = doSchemasMatch(dsPair)
    schemaMatchCheckResult match {
      case Left(errMsg) => RawCheckResult(CheckStatus.Error, errMsg)
      case Right(schema) =>
        val dsRdd = dsPair.ds.toDF.rdd
        val dsToCompareRdd = dsPair.dsToCompare.toDF.rdd
        val dsWithIndex: RDD[(Long, Row)] = zipWithIndex(dsRdd)
        val dsToCompareWithIndex: RDD[(Long, Row)] = zipWithIndex(dsToCompareRdd)
        val joined: RDD[(Option[Row], Option[Row])] = dsWithIndex.fullOuterJoin(dsToCompareWithIndex).map(_._2)
        type E[O] = Either[(Option[Row], Option[Row]), O]
        val sampleMismatchedRows: RDD[(Option[Row], Option[Row])] = joined.mapPartitions { it =>
          val allRowsInPartitionMatch: Either[(Option[Row], Option[Row]), Unit] =
            Foldable[Stream].foldM[E, (Option[Row], Option[Row]), Unit](it.toStream, ()) {
              case (_, (Some(rdd1Row), Some(rdd2Row))) =>
                if (rdd1Row != rdd2Row) {
                  Left((Some(rdd1Row), Some(rdd2Row)))
                } else {
                  Right(())
                }
              case (_, (Some(rdd1Row), None)) => Left(Some(rdd1Row), None)
              case (_, (None, Some(rdd2Row))) => Left(None, Some(rdd2Row))
            }
          allRowsInPartitionMatch match {
            case Left(firstMismatchedRow) => Iterator(firstMismatchedRow)
            case Right(_)                 => Iterator.empty
          }
        }
        sampleMismatchedRows.cache
        if (sampleMismatchedRows.isEmpty) {
          sampleMismatchedRows.unpersist(false)
          RawCheckResult(CheckStatus.Success, "Datasets are identical")
        } else {
          val sampleMismatchedRowPair = sampleMismatchedRows.first
          sampleMismatchedRows.unpersist(false)
          sampleMismatchedRowPair match {
            case (Some(dsRow), Some(dsToCompareRow)) =>
              val prettyDsRow = prettyRow(dsRow, schema)
              val prettyDsToCompareRow = prettyRow(dsToCompareRow, schema)
              RawCheckResult(
                CheckStatus.Error,
                s"Datasets encountered first mismatch at ds row: $prettyDsRow. dsToCompareRow: $prettyDsToCompareRow"
              )
            case (Some(dsRow), None) =>
              RawCheckResult(CheckStatus.Error, s"ds had extras rows, first extra row found: ${prettyRow(dsRow, schema)}")
            case (None, Some(dsToCompareRow)) =>
              RawCheckResult(CheckStatus.Error, s"dsToCompare had extras rows, first extra row found: ${prettyRow(dsToCompareRow, schema)}")
            case (None, None) =>
              throw new RuntimeException(
                "Please report this issue to the library authors, this should never happen!"
              )
          }
        }
    }
  }

  private def prettyRow(row: Row, schema: StructType): String = {
    val rowMap: Map[String, Any] = row.getValuesMap(schema.fields.map(_.name))
    prettyValuesMap(rowMap)
  }

  private def prettyValuesMap(rowMap: Map[String, Any]) = {
    val fieldStrings = rowMap.map { case (field, value) => s"$field=$value" }
    fieldStrings.mkString(", ")
  }

  private def doSchemasMatch(dsPair: DatasetPair): Either[String, StructType] = {
    val df1Fields = dsPair.ds.schema.fields.sortBy(_.name).toList
    val df2Fields = dsPair.dsToCompare.schema.fields.sortBy(_.name).toList
    if (df1Fields == df2Fields) {
      Right(dsPair.ds.schema)
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
      Left(errMsg)
    }
  }

  /**
    * Checks if schemas of 2 datasets exactly match
    */
  val dsSchemasMatch = ArbDualDsCheck("Dataset schemas match") { dsPair =>
    doSchemasMatch(dsPair) match {
      case Left(errMsg) => RawCheckResult(CheckStatus.Error, errMsg)
      case Right(_)     => RawCheckResult(CheckStatus.Success, "Dataset schemas successfully matched")
    }
  }
}
