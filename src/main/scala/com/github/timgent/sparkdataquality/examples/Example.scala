package com.github.timgent.sparkdataquality.examples

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scala.concurrent.duration._
import com.github.timgent.sparkdataquality.QualityChecker
import com.github.timgent.sparkdataquality.checks.{CheckStatus, RawCheckResult, SingleDatasetCheck}
import com.github.timgent.sparkdataquality.checks.metrics.DualMetricBasedCheck
import com.github.timgent.sparkdataquality.checks.metrics.SingleMetricBasedCheck.{ComplianceCheck, SizeCheck}
import com.github.timgent.sparkdataquality.checkssuite.{CheckSuiteStatus, ChecksSuiteBase, DescribedDataset, DualDatasetMetricChecks, ChecksSuite, MetricsBasedChecksSuite, SingleDatasetCheckWithDs, SingleDatasetChecksSuite, SingleDatasetMetricChecks}
import com.github.timgent.sparkdataquality.examples.ExampleHelpers.{Customer, Order}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.{DistinctValuesMetricDescriptor, SizeMetricDescriptor}
import com.github.timgent.sparkdataquality.metrics.{ComplianceFn, MetricComparator, MetricDescriptor}
import com.github.timgent.sparkdataquality.repository.{ElasticSearchMetricsPersister, ElasticSearchQcResultsRepository}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.github.timgent.sparkdataquality.utils.DateTimeUtils.InstantExtension

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import ExampleHelpers._
import com.github.timgent.sparkdataquality.examples.Day1Checks.{dualDsMetricChecks, esMetricsPersister, singleDsMetricChecks}

object ExampleHelpers {
  val sparkConf = new SparkConf().setAppName("SparkDataQualityExample").setMaster("local")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  case class Customer(customer_id: String, name: String)
  case class Order(order_id: String, customer_id: String, item: String)
  val monday = LocalDateTime.of(2020, 5, 1, 0, 0).toInstant(ZoneOffset.UTC)
  val tuesday = monday.plusDays(1)
  val wednesday = monday.plusDays(2)
  val thursday = monday.plusDays(3)
  val friday = monday.plusDays(4)
  val saturday = monday.plusDays(5)
  val sunday = monday.plusDays(6)
}

object Day1Data {

  import ExampleHelpers.spark.implicits._

  private val customerIds = (0 to 1000 by 100).map(_.toString)
  private val customers = customerIds.map(customerId => Customer(customerId, s"Jonny number $customerId"))
  private val rand = scala.util.Random
  private val orders = for {
    customerId <- customerIds
    orderIdSuffix <- 1 to 10
    orderId = s"${customerId}_$orderIdSuffix"
    numberOfPennySweets = Math.abs(rand.nextInt)
    item = s"$numberOfPennySweets penny sweets"
  } yield Order(orderId, customerId, item)
  val customerDs = DescribedDataset(customers.toDS, "customers")
  val orderDs = DescribedDataset(orders.toDS, "orders")
  val customersWithOrdersDs = DescribedDataset(customerDs.ds.join(orderDs.ds, List("customer_id"), "left"), "customerOrders")

  def showDay1Data = {
    customerDs.ds.show()
    orderDs.ds.show()
    customersWithOrdersDs.ds.show()
  }
}

object Demo extends App {
  Day1Data.showDay1Data
}

object Day1Checks extends App {

  import Day1Data._

  val esMetricsPersister = ElasticSearchMetricsPersister(List("http://127.0.0.1:9200"), "order_metrics")
  val qcResultsRepository = ElasticSearchQcResultsRepository(List("http://127.0.0.1:9200"), "orders_qc_results")

  val singleDsMetricChecks = List(
    SingleDatasetMetricChecks(customerDs, List(
      SizeCheck(AbsoluteThreshold(Some(10), Some(20))),
      ComplianceCheck(AbsoluteThreshold.exactly(1), ComplianceFn(col("name").isNotNull, "mustHaveName"))
    )),
    SingleDatasetMetricChecks(orderDs, List(SizeCheck(AbsoluteThreshold(Some(1), None)))),
    SingleDatasetMetricChecks(customersWithOrdersDs, List(SizeCheck(AbsoluteThreshold(Some(1), None))))
  )

  val dualDsMetricChecks = List(
    DualDatasetMetricChecks(customerDs, customersWithOrdersDs, List(
      DualMetricBasedCheck(SizeMetricDescriptor(), DistinctValuesMetricDescriptor(List("customer_id")), "Keep all customers")(MetricComparator.metricsAreEqual)
    )),
    DualDatasetMetricChecks(orderDs, customersWithOrdersDs, List(
      DualMetricBasedCheck(SizeMetricDescriptor(), DistinctValuesMetricDescriptor(List("order_id")), "Keep all orders")(MetricComparator.metricsAreEqual)
    ))
  )

  private val expectedCustomerColumnsCheck = SingleDatasetCheck("correctColumns") { ds =>
    val expectedColumns = Set("customer_id", "name")
    val columnsMatchExpected = expectedColumns == ds.columns.toSet
    if (columnsMatchExpected)
      RawCheckResult(CheckStatus.Success, "all columns matched")
    else
      RawCheckResult(CheckStatus.Error, s"Not all columns matched. $expectedColumns was different to ${ds.columns.toSet}")
  }

  val checksSuite = ChecksSuite("Customers and orders check suite",
    seqSingleDatasetMetricsChecks = singleDsMetricChecks,
    seqDualDatasetMetricChecks = dualDsMetricChecks,
    metricsPersister = esMetricsPersister,
    singleDatasetChecks = List(SingleDatasetCheckWithDs(customerDs, List(expectedCustomerColumnsCheck)))
  )

  val allQcResultsFuture = QualityChecker.doQualityChecks(checksSuite, qcResultsRepository, monday)
  val allQcResults = Await.result(allQcResultsFuture, 10 seconds)

  if (allQcResults.results.forall(_.overallStatus == CheckSuiteStatus.Success))
    println("All checks completed successfully!!")
  else
    println("Checks failed :(")
}
