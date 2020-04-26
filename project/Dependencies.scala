import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
  private val sparkVersion = "2.4.5"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.14.0"
  lazy val sparkCore = "org.apache.spark" % "spark-core_2.11" % sparkVersion
  lazy val sparkSql = "org.apache.spark" % "spark-sql_2.11" % sparkVersion
  lazy val deequ = "com.amazon.deequ" % "deequ" % "1.0.2"
}
