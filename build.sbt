import Dependencies.*
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy
import scala.sys.process._

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "3.4.2"

lazy val tagImage = taskKey[Unit]("Sets a GitHub actions output for the latest tag")

def tagDockerImage(imageName: String): Unit = {
  s"docker pull $imageName:${sys.env("DOCKER_TAG")}".!!
  s"docker tag $imageName:${sys.env("DOCKER_TAG")} $imageName:${sys.env("ENVIRONMENT_TAG")}".!!
  s"docker push $imageName:${sys.env("ENVIRONMENT_TAG")}".!!
}

def setupDirectories(serviceName: String) =
  Cmd(
    "RUN",
    s"""apk update && apk upgrade && apk add openjdk21-jre && \\
               |    mkdir -p /poduser/work /poduser/repo /poduser/version /poduser/database && \\
               |    chown -R 1002:1005 /poduser && \\
               |    mkdir /poduser/logs && \\
               |    touch /poduser/logs/$serviceName.log && \\
               |    chown -R nobody:nobody /poduser/logs && \\
               |    chmod 644 /poduser/logs/$serviceName.log""".stripMargin
  )

lazy val root = (project in file("."))
  .aggregate(disasterRecovery, webapp, builder, utils)
  .settings(
    publish / skip := true
  )

lazy val disasterRecovery = (project in file("disaster-recovery"))
  .enablePlugins(DockerPlugin, BuildInfoPlugin)
  .settings(tagSettings)
  .settings(commonSettings)
  .settings(
    name := "dr2-disaster-recovery",
    assembly / assemblyJarName := "disaster-recovery.jar",
    scalacOptions += "-Wunused:imports",
    libraryDependencies ++= Seq(
      preservicaClient,
      snsClient,
      sqsClient,
      fs2Core,
      ocfl
    )
  )
  .dependsOn(utils)

lazy val utils = (project in file("utils"))
  .settings(
    name := "disaster-recovery-utils",
    scalacOptions += "-Wunused:imports",
    publish / skip := true,
    libraryDependencies ++= Seq(
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      log4CatsSlf4j,
      log4Cats,
      pureConfigCatsEffect,
      pureConfig,
      scalaXml,
      doobieCore,
      sqlite
    )
  )

lazy val builder = (project in file("disaster-recovery-builder"))
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(tagSettings)
  .settings(
    name := "disaster-recovery-builder",
    scalacOptions += "-Wunused:imports",
    assembly / assemblyJarName := "disaster-recovery-db-builder.jar",
    libraryDependencies ++= Seq(
      ocfl,
      fs2,
      sqsClient
    )
  )
  .dependsOn(utils)

lazy val webapp = (project in file("disaster-recovery-webapp"))
  .enablePlugins(SbtTwirl, DockerPlugin)
  .settings(commonSettings)
  .settings(tagSettings)
  .settings(
    assembly / assemblyJarName := "disaster-recovery-webapp.jar",
    name := "disaster-recovery-webapp",
    libraryDependencies ++= Seq(
      http4sEmber,
      http4sDsl,
      ocfl
    )
  )
  .dependsOn(utils)

lazy val tagSettings = Seq(
  tagImage := tagDockerImage(s"${dockerRepository.value.get}/${(Docker / packageName).value}")
)

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
    case PathList("META-INF", xs @ _*) =>
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
    val filtered = universalMappings.filter { case (file, name) =>
      !name.endsWith(".jar")
    }
    filtered :+ (fatJar -> ("lib/" + fatJar.getName))
  },
  dockerRepository := Some(s"${sys.env.getOrElse("MANAGEMENT_ACCOUNT_NUMBER", "")}.dkr.ecr.eu-west-2.amazonaws.com"),
  dockerBuildOptions ++= Seq("--no-cache", "--pull"),
  Docker / packageName := s"dr2-${baseDirectory.value.getName}",
  Docker / version := sys.env.getOrElse("DOCKER_TAG", version.value),
  dockerCommands := Seq(
    Cmd("FROM", "alpine"),
    setupDirectories(name.value),
    Cmd("COPY", s"2/opt/docker/lib/${(assembly / assemblyJarName).value}", s"/opt/${(assembly / assemblyJarName).value}"),
    Cmd("USER", "1002"),
    ExecCmd("CMD", "java", "-jar", s"/opt/${(assembly / assemblyJarName).value}")
  )
)
