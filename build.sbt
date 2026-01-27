import Dependencies.*
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

import scala.sys.process.*

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "3.8.1"

lazy val tagImage = taskKey[Unit]("Sets a GitHub actions output for the latest tag")
lazy val tagScannedImage = taskKey[Unit]("Tags the image with the Wiz CLI")
lazy val scanDockerImage = taskKey[Unit]("Uses the Wiz CLI to scan the image")

def tagDockerImage(imageName: String): Unit = {
  s"docker pull $imageName:${sys.env("DOCKER_TAG")}".!!
  s"docker tag $imageName:${sys.env("DOCKER_TAG")} $imageName:${sys.env("ENVIRONMENT_TAG")}".!!
  s"docker push $imageName:${sys.env("ENVIRONMENT_TAG")}".!!
}

def getWizPath = Path(sys.env.getOrElse("WIZ_CLI_PATH", "./wizcli")).absolutePath

def scanDockerImage(imageName: String): Unit = {
  s"$getWizPath scan container-image $imageName:${sys.env("DOCKER_TAG")}".!!
}

def tagScannedImage(imageName: String): Unit = {
  s"$getWizPath tag $imageName:${sys.env("DOCKER_TAG")}".!!
}

def setupDirectories(serviceName: String) =
  Cmd(
    "RUN",
    s"""apk update && apk upgrade && apk update openssl && apk add openjdk21-jre && \\
               |    mkdir -p /poduser/work /poduser/repo /poduser/version /poduser/database /poduser/log-config && \\
               |    mkdir /poduser/logs && \\
               |    touch /poduser/logs/$serviceName.log && \\
               |    chmod 644 /poduser/logs/$serviceName.log && \\
               |    chown -R 1002:1005 /poduser
               |    """.stripMargin
  )

lazy val root = (project in file("."))
  .aggregate(custodialCopyBackend, webapp, builder, confirmer, reconciler, reIndexer, utils)
  .settings(
    publish / skip := true
  )

lazy val custodialCopyBackend = (project in file("custodial-copy-backend"))
  .enablePlugins(DockerPlugin, BuildInfoPlugin)
  .settings(imageSettings)
  .settings(commonSettings)
  .settings(
    name := "custodial-copy-backend",
    assembly / assemblyJarName := "custodial-copy.jar",
    scalacOptions += "-Wunused:imports",
    libraryDependencies ++= Seq(
      h2,
      preservicaClient,
      snsClient,
      sqsClient,
      fs2Core
    )
  )
  .dependsOn(utils)

lazy val utils = (project in file("utils"))
  .settings(
    name := "custodial-copy-utils",
    scalacOptions += "-Wunused:imports",
    publish / skip := true,
    libraryDependencies ++= Seq(
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      log4CatsSlf4j,
      log4Cats,
      ocfl,
      pureConfigCatsEffect,
      pureConfig,
      scalaXml,
      doobieCore,
      sqlite,
      sqsClient
    )
  )

lazy val reIndexer = (project in file("custodial-copy-re-indexer"))
  .enablePlugins(UniversalPlugin, JavaAppPackaging)
  .settings(commonSettings)
  .settings(imageSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      declineEffect
    ),
    dockerCommands := dockerCommands.value.dropRight(1) :+ ExecCmd("ENTRYPOINT", "java", "-Xmx2g", "-jar", s"/opt/${(assembly / assemblyJarName).value}")
  )

lazy val builder = (project in file("custodial-copy-db-builder"))
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(imageSettings)
  .settings(
    name := "custodial-copy-db-builder",
    scalacOptions += "-Wunused:imports",
    assembly / assemblyJarName := "custodial-copy-db-builder.jar",
    libraryDependencies ++= Seq(
      fs2Core,
      sqsClient
    )
  )
  .dependsOn(utils)

lazy val confirmer = (project in file("custodial-copy-confirmer"))
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(imageSettings)
  .settings(
    name := "custodial-copy-confirmer",
    scalacOptions += "-Wunused:imports",
    assembly / assemblyJarName := "custodial-copy-confirmer.jar",
    libraryDependencies ++= Seq(
      fs2Core,
      dynamoClient
    )
  )
  .dependsOn(utils)

lazy val webapp = (project in file("custodial-copy-webapp"))
  .enablePlugins(SbtTwirl, DockerPlugin)
  .settings(commonSettings)
  .settings(imageSettings)
  .settings(
    assembly / assemblyJarName := "custodial-copy-webapp.jar",
    name := "custodial-copy-webapp",
    libraryDependencies ++= Seq(
      http4sEmber,
      http4sDsl
    )
  )
  .dependsOn(utils)

lazy val reconciler = (project in file("custodial-copy-reconciler"))
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(imageSettings)
  .settings(
    name := "custodial-copy-reconciler",
    scalacOptions += "-Wunused:imports",
    assembly / assemblyJarName := "custodial-copy-reconciler.jar",
    libraryDependencies ++= Seq(
      eventbridgeClient,
      fs2Core,
      h2,
      preservicaClient
    )
  )
  .dependsOn(utils)

lazy val imageSettings = {
  Seq(
    tagImage := tagDockerImage(s"${dockerRepository.value.get}/${(Docker / packageName).value}"),
    scanDockerImage := scanDockerImage(s"${dockerRepository.value.get}/${(Docker / packageName).value}"),
    tagScannedImage := tagScannedImage(s"${dockerRepository.value.get}/${(Docker / packageName).value}")
  )
}

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    scalaCheck % Test,
    scalaCheckPlus % Test,
    scalaTest % Test,
    mockito % Test,
    wiremock % Test
  ),
  scalacOptions ++= Seq("-Werror", "-deprecation", "-feature", "-language:implicitConversions"),
  (Test / fork) := true,
  (Test / envVars) := Map(
    "AWS_ACCESS_KEY_ID" -> "accesskey",
    "AWS_SECRET_ACCESS_KEY" -> "secret",
    "AWS_LAMBDA_FUNCTION_NAME" -> "test"
  ),
  (assembly / assemblyMergeStrategy) := {
    case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
    case PathList("META-INF", xs @ _*)                       =>
      xs map {
        _.toLowerCase
      } match {
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
    case manifest if manifest.contains("MANIFEST.MF") => MergeStrategy.discard
    case x                                            => MergeStrategy.last
  },
  Universal / mappings := {
    val universalMappings = (Universal / mappings).value
    val fatJar = (Compile / assembly).value
    val properties = new java.io.File(s"${(Compile / resourceDirectory).value}/log4j2.properties")
    val filtered = universalMappings.filter { case (file, name) =>
      !name.endsWith(".jar")
    }
    filtered ++ Seq(fatJar -> ("lib/" + fatJar.getName), properties -> "lib/log4j2.properties")
  },
  dockerRepository := Some(s"${sys.env.getOrElse("MANAGEMENT_ACCOUNT_NUMBER", "")}.dkr.ecr.eu-west-2.amazonaws.com"),
  dockerBuildOptions ++= Seq("--no-cache", "--pull"),
  Docker / packageName := s"dr2-${baseDirectory.value.getName}",
  Docker / version := sys.env.getOrElse("DOCKER_TAG", version.value),
  dockerCommands := Seq(
    Cmd("FROM", "alpine"),
    setupDirectories(name.value),
    Cmd("COPY", s"2/opt/docker/lib/log4j2.properties", "/poduser/log-config/log4j2.properties"),
    Cmd("COPY", s"2/opt/docker/lib/${(assembly / assemblyJarName).value}", s"/opt/${(assembly / assemblyJarName).value}"),
    Cmd("USER", "1002"),
    Cmd(
      "CMD",
      s"[ -z $$DOWNLOAD_DIR ] || rm -rf $$DOWNLOAD_DIR && java -Xmx2g -Dlog4j.configurationFile=/poduser/log-config/log4j2.properties -jar /opt/${(assembly / assemblyJarName).value}"
    )
  )
)
