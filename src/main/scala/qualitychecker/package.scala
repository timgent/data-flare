import com.amazon.deequ.checks.{Check, CheckResult, CheckStatus}
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.repository.MetricsRepository

package object qualitychecker {
  type DeequCheck = Check
  type DeequCheckResult = CheckResult
  type DeequCheckStatus = CheckStatus.Value
  val DeequCheckStatus = CheckStatus
  type DeequMetricsRepository = MetricsRepository
  type DeequConstraint = Constraint
}
