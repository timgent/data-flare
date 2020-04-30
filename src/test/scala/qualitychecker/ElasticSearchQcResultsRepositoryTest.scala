package qualitychecker

import com.sksamuel.elastic4s.testkit.DockerTests
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import qualitychecker.CheckResultDetails.{NoDetails, NoDetailsT}
import qualitychecker.checks.{CheckResult, CheckStatus}
import qualitychecker.repository.ElasticSearchQcResultsRepository
import utils.CommonFixtures._

class ElasticSearchQcResultsRepositoryTest extends AnyWordSpec with Matchers with DockerTests {
  "ElasticSearchQcResultsRepository.save" should {
    def generateRawCheckResult(suffix: String, status: CheckStatus) = CheckResult(status, s"checkResult$suffix", s"checkDescription$suffix")
    val someIndex = "index_name"
    createIdx(someIndex)
    "Append check suite results to the index" in {
      val repo: ElasticSearchQcResultsRepository = new ElasticSearchQcResultsRepository(client, someIndex)

      val checkResultA1 = generateRawCheckResult("A1", CheckStatus.Success)
      val checkResultA2 = generateRawCheckResult("A2", CheckStatus.Success)
      val checkResultB1 = generateRawCheckResult("B1", CheckStatus.Error)
      val checkResultB2 = generateRawCheckResult("B2", CheckStatus.Error)
      val checkResultB1Success = generateRawCheckResult("B1", CheckStatus.Error)
      val checkResultB2Success = generateRawCheckResult("B2", CheckStatus.Error)
      val initialResultsToInsert: Seq[ChecksSuiteResult[NoDetailsT]] = Seq(
        ChecksSuiteResult(CheckSuiteStatus.Success, "checkSuiteA", "resultA", Seq(checkResultA1, checkResultA2), now, QcType.SingleDatasetQualityCheck, someTags, NoDetails),
        ChecksSuiteResult(CheckSuiteStatus.Error, "checkSuiteB", "resultB", Seq(checkResultB1, checkResultB2), now, QcType.DatasetComparisonQualityCheck, someTags, NoDetails)
      )
      val moreResultsToInsert: Seq[ChecksSuiteResult[NoDetailsT]] = Seq(
        ChecksSuiteResult(CheckSuiteStatus.Success, "checkSuiteB", "resultB", Seq(checkResultB1Success, checkResultB2Success),
          now.plusSeconds(10), QcType.DatasetComparisonQualityCheck, someTags, NoDetails)
      )

      repo.save(initialResultsToInsert)
      repo.save(moreResultsToInsert)

      val storedResults: Seq[ChecksSuiteResult[NoDetailsT]] = repo.loadAll
      storedResults shouldBe initialResultsToInsert
    }
  }
}
