import sbt._
object Dependencies {
  lazy val logbackVersion = "2.23.1"
  private val log4CatsVersion = "2.6.0"

  lazy val fs2Core = "co.fs2" %% "fs2-core" % "3.9.3"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val log4Cats = "org.typelevel" %% "log4cats-core" % log4CatsVersion
  lazy val log4CatsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion
  lazy val ocfl = "io.ocfl" % "ocfl-java-core" % "2.0.0"
  lazy val preservicaClient = "uk.gov.nationalarchives" %% "preservica-client-fs2" % "0.0.58"
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % "0.17.5"
  lazy val pureConfigCatsEffect = "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.6"
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.2"
  lazy val lambdaJavaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.11.1"
  lazy val sqsClient = "uk.gov.nationalarchives" %% "da-sqs-client" % "0.1.43"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15"
  lazy val mockito = "org.mockito" %% "mockito-scala" % "1.17.30"
}
