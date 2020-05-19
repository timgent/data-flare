import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
  private val sparkVersion = "2.4.5"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.14.0"
  lazy val sparkCore = "org.apache.spark" % "spark-core_2.11" % sparkVersion
  lazy val sparkSql = "org.apache.spark" % "spark-sql_2.11" % sparkVersion
  lazy val deequ = "com.amazon.deequ" % "deequ" % "1.0.2"
  private val elastic4sVersion = "7.1.0"
  lazy val elastic4s = "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion
  lazy val elastic4sTestKit = "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test"
  lazy val elastic4sCirceJson = "com.sksamuel.elastic4s" % "elastic4s-json-circe_2.11" % elastic4sVersion
  lazy val enumeratum = "com.beachape" %% "enumeratum" % "1.5.15"
  lazy val cats = "org.typelevel" %% "cats-core" % "2.0.0"
}
