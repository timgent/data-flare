import Dependencies._
import xerial.sbt.Sonatype.GitHubHosting

ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1.2"
ThisBuild / organization := "com.github.timgent"
ThisBuild / organizationName := "timgent"

lazy val root = (project in file("."))
  .settings(
    name := "spark-data-quality",
    libraryDependencies ++= List(
      scalaTest % Test,
      sparkTestingBase % Test,
      sparkCore,
      sparkSql,
      deequ,
      elastic4s,
      elastic4sTestKit,
      elastic4sCirceJson,
      enumeratum
    ),
    fork in Test := true,
    parallelExecution in Test := false,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
  )

developers := List(Developer("timgent", "Tim Gent", "tim.gent@gmail.com", url("https://github.com/timgent")))
scmInfo := Some(ScmInfo(
  url("https://github.com/timgent/spark-data-quality.git"),
  "scm:git@github.com:timgent/spark-data-quality.git"
))
homepage := Some(url("https://github.com/timgent/spark-data-quality"))
licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
publishTo := sonatypePublishToBundle.value
sonatypeProfileName := "com.github.timgent"
publishMavenStyle := true
sonatypeProjectHosting := Some(GitHubHosting("timgent", "spark-data-quality", "tim.gent@gmail.com"))


// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
