package uk.gov.nationalarchives.custodialcopy

import cats.effect.*
import cats.effect.std.Semaphore
import cats.implicits.*
import fs2.Stream
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.*
import pureconfig.module.catseffect.syntax.*
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.custodialcopy.Message.*
import uk.gov.nationalarchives.custodialcopy.Processor.Result.*
import uk.gov.nationalarchives.DASNSClient
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.custodialcopy.Processor.Result

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

  case class IdWithSourceAndDestPaths(id: UUID, sourceNioFilePath: Option[file.Path], destinationPath: String)

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

  private def aggregateMessages(sqs: DASQSClient[IO], config: Config): IO[List[MessageResponse[ReceivedSnsMessage]]] = {
    def collectMessages(messages: List[MessageResponse[ReceivedSnsMessage]]): IO[List[MessageResponse[ReceivedSnsMessage]]] = {
      sqs
        .receiveMessages[ReceivedSnsMessage](config.sqsQueueUrl)
        .flatMap { newMessages =>
          val allMessages = newMessages ++ messages
          if newMessages.isEmpty || allMessages.size >= 50 then IO.pure(allMessages) else collectMessages(allMessages)
        }
        .handleErrorWith { err =>
          logError(err) >> IO.pure[List[MessageResponse[ReceivedSnsMessage]]](messages)
        }
    }
    collectMessages(Nil)
  }

  def runCustodialCopy(sqs: DASQSClient[IO], config: Config, processor: Processor): Stream[IO, List[Result]] =
    Stream
      .eval(aggregateMessages(sqs, config))
      .filter(messageResponses => messageResponses.nonEmpty)
      .evalMap { messageResponses =>
        messageResponses
          .groupBy(_.messageGroupId)
          .toList
          .parTraverse { case (potentialMessageGroupId, responses) =>
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

      results <- dedupedMessages.traverse(processor.process)

      _ <- processor.commitStagedChanges(UUID.fromString(groupId))
      _ <- logger.info(s"${results.count(_.isSuccess)} messages out of ${results.length} unique messages processed successfully")
      _ <- logger.info(s"${results.count(_.isError)} messages out of ${results.length} unique messages failed")

      _ <- results.parTraverse {
        case Failure(e) => logError(e)
        case Success(ref) =>
          responses
            .filter(_.message.ref == ref)
            .map(_.receiptHandle)
            .parTraverse(processor.deleteMessage)
      }
    } yield results
  }

  private def dedupeMessages(messages: List[MessageResponse[ReceivedSnsMessage]]): List[MessageResponse[ReceivedSnsMessage]] =
    messages.distinctBy(_.message.ref)

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  } yield ()
}
