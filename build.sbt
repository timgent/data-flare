import Dependencies._
import xerial.sbt.Sonatype.GitHubHosting

val sparkVersion = settingKey[String]("Spark version")

val currVersion = "0.1.12"

ThisBuild / organization := "com.github.timgent"
ThisBuild / organizationName := "timgent"

lazy val root = (project in file("."))
  .settings(
    name := "data-flare",
    sparkVersion := System.getProperty("sparkVersion", "2.4.5"),
    scalaVersion := {
      if (sparkVersion.value >= "2.4.0")
        "2.12.10"
      else
        "2.11.11"
    },
    crossScalaVersions := {
      if (sparkVersion.value >= "2.4.0")
        Seq("2.12.10", "2.11.11")
      else
        Seq("2.11.11")
    },
    version := s"${sparkVersion.value}_$currVersion",
    libraryDependencies ++= sparkDependencies(sparkVersion.value) ++ List(
      scalaTest,
      scalaMock,
      elastic4s,
      elastic4sTestKit,
      elastic4sCirceJson,
      enumeratum,
      cats,
      spire,
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

lazy val docs = project // new documentation project
  .in(file("data-flare-docs")) // important: it must not be docs/
  .dependsOn(root)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
  .settings(
    moduleName := "data-flare-docs",
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(root),
    target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
    cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(unidoc in Compile).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(unidoc in Compile).value,
    mdocIn := new File("docs-source"),
    mdocVariables := Map("VERSION" -> currVersion)
  )

scalacOptions += "-Ypartial-unification"
developers := List(Developer("timgent", "Tim Gent", "tim.gent@gmail.com", url("https://github.com/timgent")))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/timgent/data-flare.git"),
    "scm:git@github.com:timgent/data-flare.git"
  )
)
homepage := Some(url("https://github.com/timgent/data-flare"))
licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
publishTo := sonatypePublishToBundle.value
sonatypeProfileName := "com.github.timgent"
publishMavenStyle := true
sonatypeProjectHosting := Some(GitHubHosting("timgent", "data-flare", "tim.gent@gmail.com"))
