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
  "ArbDualDsCheck.dsSchemasMatch" should {
    "return an error if the schemas do not match, including details of the mismatch" in {
      val df1 = DescribedDs(spark.emptyDataset[SampleData1], "df1")
      val df2 = DescribedDs(spark.emptyDataset[SampleData2], "df2")
      val dsPair = DescribedDsPair(df1, df2)
      val checkResult = ArbDualDsCheck.dsSchemasMatch.applyCheck(dsPair)
      checkResult.qcType shouldBe QcType.ArbDualDsCheck
      checkResult.status shouldBe CheckStatus.Error
      checkResult.resultDescription should (include("Dataset schemas did not match") and
        include("Column c types did not match (StringType vs IntegerType)") and
        include("Column d was present only in ds") and
        include("Column e was present only in dsToCompare"))
      checkResult.checkDescription shouldBe SimpleCheckDescription("Dataset schemas match")
      checkResult.datasourceDescription shouldBe Some(DualDsDescription("df1", "df2"))
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
}
