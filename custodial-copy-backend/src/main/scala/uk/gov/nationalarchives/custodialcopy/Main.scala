package uk.gov.nationalarchives.custodialcopy

import cats.effect.*
import cats.effect.std.Semaphore
import cats.implicits.*
import cats.syntax.all.*
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
      _ <- {
        Stream.fixedRateStartImmediately[IO](10.seconds) >>
          runCustodialCopy(sqs, config, processor)
            .map(outcomes => if outcomes.exists(_.isError) then semaphore.release else IO.unit)
            .handleErrorWith(err => Stream.eval(semaphore.release >> logError(err)))
      }.compile.drain
    } yield ExitCode.Success

  def runCustodialCopy(sqs: DASQSClient[IO], config: Config, processor: Processor): Stream[IO, List[Outcome[IO, Throwable, UUID]]] =
    Stream
      .eval(sqs.receiveMessages[ReceivedSnsMessage](config.sqsQueueUrl))
      .filter(messageResponses => messageResponses.nonEmpty)
      .evalMap { messageResponses =>
        messageResponses
          .groupBy(_.messageGroupId)
          .toList
          .traverse { case (potentialMessageGroupId, responses) =>
            potentialMessageGroupId match
              case Some(groupId) =>
                processMessages(processor, responses, groupId)
              case None => IO.raiseError(new Exception("Message Group ID is missing"))
          }
          .map(_.flatten)

      }

  private def processMessages(processor: Processor, responses: List[MessageResponse[ReceivedSnsMessage]], groupId: String) = {
    for {
      logger <- Slf4jLogger.create[IO]
      _ <- logger.info(s"Processing ${responses.length} messages")
      _ <- logger.info(responses.map(_.message.ref).mkString(","))
      dedupedMessages = dedupeMessages(responses)
      (deletedEntities, nonDeletedEntities) = dedupedMessages.partition(_.message.deleted)

      fibersForNonDeletedEntities <- nonDeletedEntities.parTraverse(processor.process(_, false).start)
      ndeFiberResults <- fibersForNonDeletedEntities.traverse(_.join)

      fibersForDeletedEntities <- deletedEntities.parTraverse(processor.process(_, true).start)
      deFiberResults <- fibersForDeletedEntities.traverse(_.join)

      allFibers = fibersForNonDeletedEntities ++ fibersForDeletedEntities
      allFiberResults = ndeFiberResults ++ deFiberResults

      _ <- processor.commitStagedChanges(UUID.fromString(groupId))
      _ <- logger.info(s"${allFiberResults.count(_.isSuccess)} messages out of ${allFibers.length} unique messages processed successfully")
      _ <- logger.info(s"${allFiberResults.count(_.isError)} messages out of ${allFibers.length} unique messages failed")

      _ <- allFiberResults traverse {
        case Outcome.Errored(e) => logError(e) >> IO.unit
        case Outcome.Succeeded(refIO) =>
          refIO.flatMap { ref =>
            responses
              .filter(_.message.ref == ref)
              .map(_.receiptHandle)
              .parTraverse(processor.deleteMessage)
          }
        case _ => IO.unit
      }
    } yield allFiberResults
  }

  private def dedupeMessages(messages: List[MessageResponse[ReceivedSnsMessage]]): List[MessageResponse[ReceivedSnsMessage]] =
    messages.distinctBy(_.message.ref)

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  } yield ()
}
