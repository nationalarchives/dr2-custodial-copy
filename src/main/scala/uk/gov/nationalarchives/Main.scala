package uk.gov.nationalarchives

import cats.effect._
import fs2.Stream
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.Message._
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
      proxyUrl: Option[URI]
  )

  private def sqsClient(config: Config): DASQSClient[IO] =
    config.proxyUrl
      .map(proxy => DASQSClient[IO](proxy))
      .getOrElse(DASQSClient[IO]())

  implicit val decoder: Decoder[Option[Message]] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[String]
    } yield {
      val typeAndRef = id.split(":")
      val ref = UUID.fromString(typeAndRef.last)
      val entityType = typeAndRef.head
      entityType match {
        case "io" => Option(InformationObjectMessage(ref, id))
        case "co" => Option(ContentObjectMessage(ref, id))
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
      processor <- Processor(config, sqs, service, client)
      _ <- {
        Stream.fixedRateStartImmediately[IO](20.seconds) >>
          runDisasterRecovery(sqs, config, processor)
            .handleErrorWith(err => Stream.eval(logError(err)))
      }.compile.drain
    } yield ExitCode.Success

  def runDisasterRecovery(sqs: DASQSClient[IO], config: Config, processor: Processor): Stream[IO, Unit] =
    Stream
      .eval(sqs.receiveMessages[Option[Message]](config.sqsQueueUrl))
      .evalMap(messages => IO.whenA(messages.nonEmpty)(processor.process(messages)))

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running disaster recovery")
  } yield ()
}
