package com.github.timgent.sparkdataquality.examples

import java.time.{LocalDateTime, ZoneOffset}

import com.github.timgent.sparkdataquality.checks.metrics.{DualMetricBasedCheck, SingleMetricBasedCheck}
import com.github.timgent.sparkdataquality.checks.{CheckStatus, RawCheckResult, SingleDatasetCheck}
import com.github.timgent.sparkdataquality.checkssuite._
import com.github.timgent.sparkdataquality.examples.Day1Checks.qcResults
import com.github.timgent.sparkdataquality.examples.ExampleHelpers.{Customer, Order, _}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.{CountDistinctValuesMetric, SizeMetric}
import com.github.timgent.sparkdataquality.metrics.{ComplianceFn, MetricComparator}
import com.github.timgent.sparkdataquality.repository.{ElasticSearchMetricsPersister, ElasticSearchQcResultsRepository}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold
import com.github.timgent.sparkdataquality.utils.DateTimeUtils.InstantExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

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
  private val customers =
    customerIds.map(customerId => Customer(customerId, s"Jonny number $customerId"))
  private val rand = scala.util.Random
  private val orders = for {
    customerId <- customerIds
    orderIdSuffix <- 1 to 2
    orderId = s"${customerId}_$orderIdSuffix"
    numberOfPennySweets = Math.abs(rand.nextInt)
    item = s"$numberOfPennySweets penny sweets"
  } yield Order(orderId, customerId, item)
  val customerDs = DescribedDataset(customers.toDS, "customers")
  val orderDs = DescribedDataset(orders.toDS, "orders")
  val customersWithOrdersDs =
    DescribedDataset(customerDs.ds.join(orderDs.ds, List("customer_id"), "left"), "customerOrders")

  def showData = {
    customerDs.ds.show(10, false)
    orderDs.ds.show(10, false)
    customersWithOrdersDs.ds.show(10, false)
  }
}

object Day2Data {

  import ExampleHelpers.spark.implicits._

  private val customerIds = (0 to 1000 by 100).map(_.toString)
  private val customers =
    customerIds.map(customerId => Customer(customerId, s"Jonny number $customerId"))
  private val rand = scala.util.Random
  private val orders = for {
    customerId <- customerIds
    orderIdSuffix <- 1 to 1
    orderId = s"${customerId}_$orderIdSuffix"
    numberOfPennySweets = Math.abs(rand.nextInt)
    item = s"$numberOfPennySweets penny sweets"
  } yield Order(orderId, customerId, item)
  val customerDs = DescribedDataset(customers.toDS, "customers")
  val orderDs = DescribedDataset(orders.toDS, "orders")
  val customersWithOrdersDs =
    DescribedDataset(customerDs.ds.join(orderDs.ds, List("customer_id"), "left"), "customerOrders")

  def showData = {
    customerDs.ds.show(10, false)
    orderDs.ds.show(10, false)
    customersWithOrdersDs.ds.show(10, false)
  }
}

object Day3Data {

  import ExampleHelpers.spark.implicits._

  private val customerIds = (0 to 1000 by 100).map(_.toString)
  private val customers =
    customerIds.map(customerId => Customer(customerId, s"Jonny number $customerId"))
  private val rand = scala.util.Random
  private val orders = for {
    customerId <- customerIds.tail
    orderIdSuffix <- 1 to 2
    orderId = s"${customerId}_$orderIdSuffix"
    numberOfPennySweets = Math.abs(rand.nextInt)
    item = s"$numberOfPennySweets penny sweets"
  } yield Order(orderId, customerId, item)
  val customerDs = DescribedDataset(customers.toDS, "customers")
  val orderDs = DescribedDataset(orders.toDS, "orders")
  val customersWithOrdersDs =
    DescribedDataset(customerDs.ds.join(orderDs.ds, List("customer_id"), "inner"), "customerOrders")

  def showData = {

    customerDs.ds.show(10, false)
    orderDs.ds.show(10, false)
    customersWithOrdersDs.ds.show(10, false)
  }
}

object DemoIntro extends App {
  Day1Data.showData
  Day2Data.showData
  Day3Data.showData
}

object Helpers {
  val qcResultsRepository =
    ElasticSearchQcResultsRepository(List("http://127.0.0.1:9200"), "orders_qc_results")
  val esMetricsPersister =
    ElasticSearchMetricsPersister(List("http://127.0.0.1:9200"), "order_metrics")

  def getCheckSuite(
      orderDs: DescribedDataset,
      customerDs: DescribedDataset,
      customersWithOrdersDs: DescribedDataset
  ): ChecksSuite = {

    val singleDsMetricChecks = List(
      SingleDatasetMetricChecks(
        customerDs,
        List(
          SingleMetricBasedCheck.sizeCheck(AbsoluteThreshold(Some(10L), Some(20L))),
          SingleMetricBasedCheck.complianceCheck(
            AbsoluteThreshold.exactly(1),
            ComplianceFn(col("name").isNotNull, "mustHaveName")
          )
        )
      ),
      SingleDatasetMetricChecks(orderDs, List(SingleMetricBasedCheck.sizeCheck(AbsoluteThreshold(Some(1L), None)))),
      SingleDatasetMetricChecks(
        customersWithOrdersDs,
        List(SingleMetricBasedCheck.sizeCheck(AbsoluteThreshold(Some(1L), None)))
      )
    )

    val dualDsMetricChecks = List(
      DualDatasetMetricChecks(
        customerDs,
        customersWithOrdersDs,
        List(
          DualMetricBasedCheck(
            SizeMetric(),
            CountDistinctValuesMetric(List("customer_id")),
            "Keep all customers",
            MetricComparator.metricsAreEqual)
        )
      ),
      DualDatasetMetricChecks(
        orderDs,
        customersWithOrdersDs,
        List(
          DualMetricBasedCheck(
            SizeMetric(),
            CountDistinctValuesMetric(List("order_id")),
            "Keep all orders",
            MetricComparator.metricsAreEqual)
        )
      )
    )

    val expectedCustomerColumnsCheck = SingleDatasetCheck("correctColumns") { ds =>
      val expectedColumns = Set("customer_id", "name")
      val columnsMatchExpected = expectedColumns == ds.columns.toSet
      if (columnsMatchExpected)
        RawCheckResult(CheckStatus.Success, "all columns matched")
      else
        RawCheckResult(
          CheckStatus.Error,
          s"Not all columns matched. $expectedColumns was different to ${ds.columns.toSet}"
        )
    }

    val checksSuite = ChecksSuite(
      "Customers and orders check suite",
      singleDatasetMetricChecks = singleDsMetricChecks,
      dualDatasetMetricChecks = dualDsMetricChecks,
      metricsPersister = esMetricsPersister,
      singleDatasetChecks = List(SingleDatasetCheckWithDs(customerDs, List(expectedCustomerColumnsCheck))),
      qcResultsRepository = qcResultsRepository
    )

    checksSuite
  }
}

object Day1Checks extends App {

  import Day1Data._
  import Helpers.qcResultsRepository

  val checksSuite = Helpers.getCheckSuite(orderDs, customerDs, customersWithOrdersDs)

  val allQcResultsFuture = checksSuite.run(monday)
  val qcResults = Await.result(allQcResultsFuture, 10 seconds)

  if (qcResults.overallStatus == CheckSuiteStatus.Success)
    println("All checks completed successfully!!")
  else
    println("Checks failed :(")
}

object Day2Checks extends App {

  import Day2Data._
  import Helpers.qcResultsRepository

  val checksSuite = Helpers.getCheckSuite(orderDs, customerDs, customersWithOrdersDs)

  val allQcResultsFuture = checksSuite.run(tuesday)
  val allQcResults = Await.result(allQcResultsFuture, 10 seconds)

  if (qcResults.overallStatus == CheckSuiteStatus.Success)
    println("All checks completed successfully!!")
  else
    println("Checks failed :(")
}

object Day3Checks extends App {

  import Day3Data._
  import Helpers.qcResultsRepository

  val checksSuite = Helpers.getCheckSuite(orderDs, customerDs, customersWithOrdersDs)

  val allQcResultsFuture = checksSuite.run(wednesday)
  val allQcResults = Await.result(allQcResultsFuture, 10 seconds)

  if (qcResults.overallStatus == CheckSuiteStatus.Success)
    println("All checks completed successfully!!")
  else
    println("Checks failed :(")
}
