package com.github.timgent.dataflare.checks

import com.github.timgent.dataflare.checks.CheckDescription.SimpleCheckDescription
import com.github.timgent.dataflare.checks.DatasourceDescription.DualDsDescription
import com.github.timgent.dataflare.checkssuite.{DescribedDs, DescribedDsPair}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class SampleData1(a: String, b: String, c: String, d: Int)

case class SampleData2(a: String, b: String, c: Int, e: String)

class ArbDualDsCheckTest extends AnyWordSpec with DatasetSuiteBase with Matchers {
  import spark.implicits._

  private def assertCheckReturnsSchemaErrorForSchemaMismatch(check: ArbDualDsCheck, checkDescription: String) = {
    val df1 = DescribedDs(spark.emptyDataset[SampleData1], "df1")
    val df2 = DescribedDs(spark.emptyDataset[SampleData2], "df2")
    val dsPair = DescribedDsPair(df1, df2)
    val checkResult = check.applyCheck(dsPair)
    checkResult.qcType shouldBe QcType.ArbDualDsCheck
    checkResult.status shouldBe CheckStatus.Error
    checkResult.resultDescription should (include("Dataset schemas did not match") and
      include("Column c types did not match (StringType vs IntegerType)") and
      include("Column d was present only in ds") and
      include("Column e was present only in dsToCompare"))
    checkResult.checkDescription shouldBe SimpleCheckDescription(checkDescription)
    checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
  }

  "ArbDualDsCheck.dsSchemasMatch" should {
    "return an error if the schemas do not match, including details of the mismatch" in {
      assertCheckReturnsSchemaErrorForSchemaMismatch(ArbDualDsCheck.dsSchemasMatch, "Dataset schemas match")
    }

    "return success if the dataset schemas match exactly" in {
      val df1 = DescribedDs(spark.emptyDataset[SampleData1], "df1")
      val df2 = DescribedDs(spark.emptyDataset[SampleData1], "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dsSchemasMatch.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Success
      checkResult.resultDescription shouldBe "Dataset schemas successfully matched"
      checkResult.checkDescription shouldBe SimpleCheckDescription("Dataset schemas match")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }
  }

  "ArbDualDsCheck.dfsMatchOrdered" should {
    "return an error if the schemas of the datasets do not match, including details of the mismatch" in {
      assertCheckReturnsSchemaErrorForSchemaMismatch(ArbDualDsCheck.dfsMatchOrdered, "Ordered dataset content matches")
    }

    "return an error if the rows in the dataset do not match, including details of the mismatch" in {
      val df1 = DescribedDs(
        List(
          SampleData1("same", "same", "same", 1),
          SampleData1("df1only", "same", "same", 2)
        ).toDS,
        "df1"
      )
      val df2 = DescribedDs(
        List(
          SampleData1("same", "same", "same", 1),
          SampleData1("df2only", "same", "same", 2)
        ).toDS,
        "df2"
      )
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchOrdered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Error
      checkResult.resultDescription shouldBe "Datasets encountered first mismatch at ds row: a=df1only, " +
        "b=same, c=same, d=2. dsToCompareRow: a=df2only, b=same, c=same, d=2"
      checkResult.checkDescription shouldBe SimpleCheckDescription("Ordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }

    "return an error if one dataset has a duplicated row that the other dataset does not" in {
      val df1 = DescribedDs(List(SampleData1("same", "same", "same", 1), SampleData1("same", "same", "same", 1)).toDS, "df1")
      val df2 = DescribedDs(List(SampleData1("same", "same", "same", 1)).toDS, "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchOrdered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Error
      checkResult.resultDescription shouldBe s"ds had extras rows, first extra row found: a=same, " +
        "b=same, c=same, d=1"
      checkResult.checkDescription shouldBe SimpleCheckDescription("Ordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }

    "return an error if the datasets have the same data in different orders" in {
      val df1 = DescribedDs(List(SampleData1("same", "same", "same", 1), SampleData1("same", "same", "same", 2)).toDS, "df1")
      val df2 = DescribedDs(List(SampleData1("same", "same", "same", 2), SampleData1("same", "same", "same", 1)).toDS, "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchOrdered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Error
      checkResult.resultDescription shouldBe s"Datasets encountered first mismatch at ds row: a=same, b=same, " +
        s"c=same, d=1. dsToCompareRow: a=same, b=same, c=same, d=2"
      checkResult.checkDescription shouldBe SimpleCheckDescription("Ordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }

    "return success if the rows in the datasets match exactly (regardless of order)" in {
      val df1 = DescribedDs(List(SampleData1("same", "same", "same", 1), SampleData1("same", "same", "same", 2)).toDS, "df1")
      val df2 = DescribedDs(
        List(
          SampleData1("same", "same", "same", 1),
          SampleData1("same", "same", "same", 2)
        ).toDS,
        "df2"
      )
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchOrdered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Success
      checkResult.resultDescription should include("Datasets are identical")
      checkResult.checkDescription shouldBe SimpleCheckDescription("Ordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }

    "return success if the datasets have the same schema but no rows" in {
      val df1 = DescribedDs(spark.emptyDataset[SampleData1], "df1")
      val df2 = DescribedDs(spark.emptyDataset[SampleData1], "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchOrdered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Success
      checkResult.resultDescription should include("Datasets are identical")
      checkResult.checkDescription shouldBe SimpleCheckDescription("Ordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }
  }

  "ArbDualDsCheck.dfsMatchUnordered" should {
    "return an error if the schemas of the datasets do not match, including details of the mismatch" in {
      assertCheckReturnsSchemaErrorForSchemaMismatch(ArbDualDsCheck.dfsMatchUnordered, "Unordered dataset content matches")
    }

    "return an error if one dataset has duplicate rows" in {
      val df1 = DescribedDs(List(SampleData1("dupe", "dupe", "dupe", 1), SampleData1("dupe", "dupe", "dupe", 1)).toDS, "df1")
      val df2 = DescribedDs(List(SampleData1("dupe", "dupe", "dupe", 1)).toDS, "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchUnordered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Error
      checkResult.resultDescription should include(
        "First mismatch was in dsToCompare where 1 row(s) had content: a=dupe, b=dupe, c=dupe, d=1"
      )
      checkResult.checkDescription shouldBe SimpleCheckDescription("Unordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }

    "return an error if the datasets have different content" in {
      val df1 = DescribedDs(List(SampleData1("diff", "diff", "diff", 2)).toDS, "df1")
      val df2 = DescribedDs(List(SampleData1("diff", "diff", "diff", 1)).toDS, "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchUnordered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Error
      checkResult.resultDescription should include(
        "First mismatch was in dsToCompare where 1 row(s) had content: a=diff, b=diff, c=diff, d=1"
      )
      checkResult.checkDescription shouldBe SimpleCheckDescription("Unordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }

    "return success if the datasets have the same data, regardless of ordering" in {
      val df1 = DescribedDs(List(SampleData1("same", "same", "same", 2), SampleData1("same", "same", "same", 1)).toDS, "df1")
      val df2 = DescribedDs(List(SampleData1("same", "same", "same", 1), SampleData1("same", "same", "same", 2)).toDS, "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchUnordered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Success
      checkResult.resultDescription should include("Datasets matched")
      checkResult.checkDescription shouldBe SimpleCheckDescription("Unordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }

    "return success if the datasets are both empty" in {
      val df1 = DescribedDs(spark.emptyDataset[SampleData1], "df1")
      val df2 = DescribedDs(spark.emptyDataset[SampleData1], "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dfsMatchUnordered.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Success
      checkResult.resultDescription should include("Datasets matched")
      checkResult.checkDescription shouldBe SimpleCheckDescription("Unordered dataset content matches")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
    }
  }
}
