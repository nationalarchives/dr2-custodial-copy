package uk.gov.nationalarchives.custodialcopy

import cats.effect.*
import cats.effect.std.Semaphore
import cats.implicits.*
import fs2.Stream
import io.circe.{Decoder, Encoder, HCursor, Json, JsonObject}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import uk.gov.nationalarchives.{DASNSClient, DASQSStreamClient}
import uk.gov.nationalarchives.DASQSStreamClient.StreamMessageResponse
import uk.gov.nationalarchives.custodialcopy.Message.*
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.net.URI
import java.nio.file
import java.util.UUID

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

  private def sqsClient(config: Config): DASQSStreamClient[IO] =
    config.proxyUrl
      .map(proxy => DASQSStreamClient[IO](proxy))
      .getOrElse(DASQSStreamClient[IO]())

  given Decoder[ReceivedSnsMessage] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[String]
      deleted <- c.downField("deleted").as[Boolean]
    } yield {
      val typeAndRef = id.split(":")
      val ref = UUID.fromString(typeAndRef.last)
      val entityType = typeAndRef.head
      entityType match {
        case "io" => IoReceivedSnsMessage(ref, deleted)
        case "co" => CoReceivedSnsMessage(ref, deleted)
        case "so" => SoReceivedSnsMessage(ref, deleted)
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
      _ <- runCustodialCopy(sqs, config, processor)
        .handleErrorWith(err => semaphore.release >> logError(err))
    } yield ExitCode.Success

  def runCustodialCopy(sqs: DASQSStreamClient[IO], config: Config, processor: Processor): IO[List[Outcome[IO, Throwable, Unit]]] = {
    sqs.receiveMessageStream[ReceivedSnsMessage]("intg-dr2-custodial-copy").use { queue =>
      Stream.fromQueueUnterminated[IO, StreamMessageResponse[ReceivedSnsMessage]](queue)
        .chunkMin(10)
        .evalMap { messageResponseChunks =>
          val messageResponses = messageResponseChunks.toList
          for {
            logger <- Slf4jLogger.create[IO]
            _ <- logger.info(s"Processing ${messageResponses.length} messages")
            _ <- logger.info(messageResponses.map(_.message.ref).mkString(","))

            (deletedEntities, nonDeletedEntities) = dedupeMessages(messageResponses).partition(_.message.deleted)

            fibersForNonDeletedEntities <- nonDeletedEntities.parTraverse(processor.process(_, false).start)
            ndeFiberResults <- fibersForNonDeletedEntities.traverse(_.join)

            fibersForDeletedEntities <- deletedEntities.parTraverse(processor.process(_, true).start)
            deFiberResults <- fibersForDeletedEntities.traverse(_.join)

            allFibers = fibersForNonDeletedEntities ++ fibersForDeletedEntities
            allFiberResults = ndeFiberResults ++ deFiberResults

            _ <- logger.info(s"${allFiberResults.count(_.isSuccess)} messages out of ${allFibers.length} unique messages processed successfully")
            _ <- logger.info(s"${allFiberResults.count(_.isError)} messages out of ${allFibers.length} unique messages failed")
            _ <- allFiberResults traverse {
              case Outcome.Errored(e) => logError(e) >> IO.unit
              case _ => IO.unit
            }
          } yield allFiberResults
        }.compile.toList.map(_.flatten)
    }
  }

  private def dedupeMessages(messages: List[StreamMessageResponse[ReceivedSnsMessage]]): List[StreamMessageResponse[ReceivedSnsMessage]] =
    messages.distinctBy(_.message.ref)

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  } yield ()
}
