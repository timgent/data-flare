import Dependencies._
import xerial.sbt.Sonatype.GitHubHosting

ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1.5"
ThisBuild / organization := "com.github.timgent"
ThisBuild / organizationName := "timgent"

lazy val root = (project in file("."))
  .settings(
    name := "spark-data-quality",
    libraryDependencies ++= List(
      scalaTest,
      sparkTestingBase,
      scalaMock,
      sparkCore,
      sparkSql,
      deequ,
      elastic4s,
      elastic4sTestKit,
      elastic4sCirceJson,
      enumeratum,
      cats,
      scalacheck,
      scalacheckToolboxDatetime,
      scalacheckToolboxMagic,
      scalacheckToolboxCombinators
    ),
    fork in Test := true,
    parallelExecution in Test := false,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    assemblyShadeRules in assembly ++= Seq(
      // Required due to conflicting shapeless versions between circe and spark libraries
      ShadeRule
        .rename("com.chuusai.shapeless.**" -> "shapeless_new.@1")
        .inLibrary("com.chuusai" %% "shapeless" % "2.3.2")
        .inProject
    )
  )

scalacOptions += "-Ypartial-unification"
developers := List(Developer("timgent", "Tim Gent", "tim.gent@gmail.com", url("https://github.com/timgent")))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/timgent/spark-data-quality.git"),
    "scm:git@github.com:timgent/spark-data-quality.git"
  )
)
homepage := Some(url("https://github.com/timgent/spark-data-quality"))
licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
publishTo := sonatypePublishToBundle.value
sonatypeProfileName := "com.github.timgent"
publishMavenStyle := true
sonatypeProjectHosting := Some(GitHubHosting("timgent", "spark-data-quality", "tim.gent@gmail.com"))
