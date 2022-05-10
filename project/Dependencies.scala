import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % Test
  lazy val scalaMock = "org.scalamock" %% "scalamock" % "4.4.0" % Test
  def sparkDependencies(sparkVersion: String) =
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.1.1" % Test
    )

  private val elastic4sVersion = "7.16.0"
  lazy val elastic4s = "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion
  lazy val elastic4sTestKit = "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test"
  lazy val elastic4sCirceJson = "com.sksamuel.elastic4s" %% "elastic4s-json-circe" % elastic4sVersion
  lazy val enumeratum = "com.beachape" %% "enumeratum" % "1.5.15"
  lazy val enumeratumCirce = "com.beachape" %% "enumeratum-circe" % "1.5.15"
  lazy val cats = "org.typelevel" %% "cats-core" % "2.0.0"
  lazy val scalacheck = "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % Test
  lazy val scalacheckToolboxDatetime = "com.47deg" %% "scalacheck-toolbox-datetime" % "0.3.5" % Test
  lazy val scalacheckToolboxMagic = "com.47deg" %% "scalacheck-toolbox-magic" % "0.3.5" % Test
  lazy val scalacheckToolboxCombinators = "com.47deg" %% "scalacheck-toolbox-combinators" % "0.3.5" % Test
  lazy val spire = "org.typelevel" %% "spire" % "0.14.1"
}
