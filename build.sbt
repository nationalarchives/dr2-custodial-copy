import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file(".")).
  settings(
    name := "dr2-disaster-recovery",
    libraryDependencies ++= Seq(
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      preservicaClient,
      sqsClient,
      fs2Core,
      ocfl,
      pureConfig,
      pureConfigCatsEffect,
      scalaTest % Test,
      mockito % Test
    ),
    scalacOptions += "-deprecation"
  )

(assembly / assemblyJarName) := "dr2-disaster-recovery.jar"

//scalacOptions ++= Seq("-Wunused:imports", "-Werror")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps@_*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case PathList("META-INF", xs@_*) =>
    xs map {_.toLowerCase} match {
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case manifest if manifest.contains("MANIFEST.MF") => MergeStrategy.discard
  case x => MergeStrategy.last
}
