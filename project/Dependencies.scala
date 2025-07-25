import sbt.*
object Dependencies {
  private lazy val daAwsClientsVersion = "0.1.125"
  private lazy val logbackVersion = "2.25.1"
  private lazy val log4CatsVersion = "2.7.1"
  private lazy val pureConfigVersion = "0.17.9"
  private lazy val scalaTestVersion = "3.2.18"
  private lazy val http4sVersion = "1.0.0-M44"

  lazy val dynamoClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % daAwsClientsVersion
  lazy val eventbridgeClient = "uk.gov.nationalarchives" %% "da-eventbridge-client" % daAwsClientsVersion
  lazy val fs2Core = "co.fs2" %% "fs2-core" % "3.12.0"
  lazy val log4Cats = "org.typelevel" %% "log4cats-core" % log4CatsVersion
  lazy val declineEffect = "com.monovore" %% "decline-effect" % "2.5.0"
  lazy val h2 = "com.h2database" % "h2" % "2.3.232"
  lazy val log4CatsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val mockito = "org.scalatestplus" %% "mockito-5-10" % s"$scalaTestVersion.0"
  lazy val ocfl = "io.ocfl" % "ocfl-java-core" % "2.2.2"
  lazy val preservicaClient = "uk.gov.nationalarchives" %% "preservica-client-fs2" % "0.0.141"
  lazy val pureConfigCatsEffect = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig-core" % pureConfigVersion
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.18.1"
  lazy val scalaCheckPlus = "org.scalatestplus" %% "scalacheck-1-16" % "3.2.14.0"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
  lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.4.0"
  lazy val snsClient = "uk.gov.nationalarchives" %% "da-sns-client" % daAwsClientsVersion
  lazy val sqsClient = "uk.gov.nationalarchives" %% "da-sqs-client" % daAwsClientsVersion
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
  lazy val http4sEmber = "org.http4s" %% "http4s-ember-server" % http4sVersion
  lazy val http4sDsl = "org.http4s" %% "http4s-dsl" % http4sVersion
  lazy val doobieCore = "org.tpolecat" %% "doobie-core" % "1.0.0-RC10"
  lazy val sqlite = "org.xerial" % "sqlite-jdbc" % "3.50.3.0"
}
