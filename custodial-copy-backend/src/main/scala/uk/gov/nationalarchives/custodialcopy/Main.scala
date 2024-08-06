package uk.gov.nationalarchives.custodialcopy

import cats.effect.*
import cats.effect.std.Semaphore
import cats.implicits.*
import fs2.Stream
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.custodialcopy.Message.*
import uk.gov.nationalarchives.DASNSClient
import uk.gov.nationalarchives.DASQSClient.MessageResponse

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
      semaphore <- Semaphore[IO](1)
      service <- OcflService(config, semaphore)
      sqs = sqsClient(config)
      sns = DASNSClient[IO]()
      processor <- Processor(config, sqs, service, client, sns)
      _ <- {
        Stream.fixedRateStartImmediately[IO](20.seconds) >>
          runCustodialCopy(sqs, config, processor)
            .handleErrorWith(err => Stream.eval(semaphore.release >> logError(err)))
      }.compile.drain
    } yield ExitCode.Success

  def runCustodialCopy(sqs: DASQSClient[IO], config: Config, processor: Processor): Stream[IO, List[Outcome[IO, Throwable, Unit]]] =
    Stream
      .eval(sqs.receiveMessages[Option[ReceivedSnsMessage]](config.sqsQueueUrl))
      .filter(messageResponses => messageResponses.nonEmpty)
      .evalMap { messageResponses =>
        for {
          logger <- Slf4jLogger.create[IO]
          _ <- logger.info(s"Processing ${messageResponses.length} messages")
          _ <- logger.info(messageResponses.flatMap(_.message.map(_.messageText)).mkString(","))
          fibers <- dedupeMessages(messageResponses).parTraverse(processor.process(_).start)
          fiberResults <- fibers.traverse(_.join)
          _ <- logger.info(s"${fiberResults.count(_.isSuccess)} messages out of ${fibers.length} unique messages processed successfully")
          _ <- logger.info(s"${fiberResults.count(_.isError)} messages out of ${fibers.length} unique messages failed")
          _ <- fiberResults traverse {
            case Outcome.Errored(e) => logError(e) >> IO.unit
            case _                  => IO.unit
          }
        } yield fiberResults
      }

  private def dedupeMessages(messages: List[MessageResponse[Option[ReceivedSnsMessage]]]): List[MessageResponse[Option[ReceivedSnsMessage]]] =
    messages.distinctBy(_.message.map(_.messageText))

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  } yield ()
}
