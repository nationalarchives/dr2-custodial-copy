package uk.gov.nationalarchives.disasterrecovery

import cats.effect.*
import fs2.Stream
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.disasterrecovery.Message.*
import uk.gov.nationalarchives.DASNSClient

import java.net.URI
import java.nio.file
import java.util.UUID
import scala.concurrent.duration.DurationInt

object Main extends IOApp {

  case class Config(
      preservicaUrl: String,
      preservicaSecretName: String,
      sqsQueueUrl: String,
      repoDir: String,
      workDir: String,
      proxyUrl: Option[URI],
      versionPath: String,
      topicArn: String
  ) derives ConfigReader

  private def sqsClient(config: Config): DASQSClient[IO] =
    config.proxyUrl
      .map(proxy => DASQSClient[IO](proxy))
      .getOrElse(DASQSClient[IO]())

  given Decoder[Option[ReceivedSnsMessage]] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[String]
    } yield {
      val typeAndRef = id.split(":")
      val ref = UUID.fromString(typeAndRef.last)
      val entityType = typeAndRef.head
      entityType match {
        case "io" => Option(IoReceivedSnsMessage(ref, id))
        case "co" => Option(CoReceivedSnsMessage(ref, id))
        case _    => None
      }
    }

  case class IdWithSourceAndDestPaths(id: UUID, sourceNioFilePath: file.Path, destinationPath: String)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      client <- Fs2Client.entityClient(
        config.preservicaUrl,
        config.preservicaSecretName,
        potentialProxyUrl = config.proxyUrl
      )
      service <- OcflService(config)
      sqs = sqsClient(config)
      sns = DASNSClient[IO]()
      processor <- Processor(config, sqs, service, client, sns)
      _ <- {
        Stream.fixedRateStartImmediately[IO](20.seconds) >>
          runDisasterRecovery(sqs, config, processor)
            .handleErrorWith(err => Stream.eval(logError(err)))
      }.compile.drain
    } yield ExitCode.Success

  def runDisasterRecovery(sqs: DASQSClient[IO], config: Config, processor: Processor): Stream[IO, Unit] =
    Stream
      .eval(sqs.receiveMessages[Option[ReceivedSnsMessage]](config.sqsQueueUrl))
      .evalMap(messages => IO.whenA(messages.nonEmpty)(processor.process(messages)))

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running disaster recovery")
  } yield ()
}
