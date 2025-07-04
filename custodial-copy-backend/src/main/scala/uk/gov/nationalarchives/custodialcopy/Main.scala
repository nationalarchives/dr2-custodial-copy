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
import uk.gov.nationalarchives.utils.OcflServiceConfig
import uk.gov.nationalarchives.utils.Utils.*

import java.net.URI
import java.nio.file
import java.util.UUID
import scala.concurrent.duration.DurationInt

object Main extends IOApp {

  case class Config(
      preservicaSecretName: String,
      sqsQueueUrl: String,
      repoDir: String,
      workDir: String,
      proxyUrl: Option[URI],
      versionPath: String,
      topicArn: String
  ) extends OcflServiceConfig derives ConfigReader

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
        config.preservicaSecretName,
        potentialProxyUrl = config.proxyUrl
      )
      semaphore <- Semaphore[IO](1)
      service <- OcflService(config, semaphore)
      sqs = sqsClient[IO](config.proxyUrl)
      sns = DASNSClient[IO]()
      processor <- Processor(config, sqs, service, client, sns)
      _ <- {
        Stream.fixedRateStartImmediately[IO](10.seconds) >>
          runCustodialCopy(sqs, config, processor)
            .map(outcomes => if outcomes.exists(_.isError) then semaphore.release else IO.unit)
            .handleErrorWith(err => Stream.eval(semaphore.release >> logError(err)))
      }.compile.drain
    } yield ExitCode.Success

  def runCustodialCopy(sqs: DASQSClient[IO], config: Config, processor: Processor): Stream[IO, List[Result]] =
    Stream
      .eval(aggregateMessages(sqs, config.sqsQueueUrl))
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
        case Failure(e) => logError[IO](e)
        case Success(ref) =>
          responses
            .filter(_.message.ref == ref)
            .map(_.receiptHandle)
            .parTraverse(receiptHandle => logger.info(s"Deleting message with receipt handle $receiptHandle") >> processor.deleteMessage(receiptHandle))
      }
    } yield results
  }

  private def dedupeMessages(messages: List[MessageResponse[ReceivedSnsMessage]]): List[MessageResponse[ReceivedSnsMessage]] =
    messages.distinctBy(_.message.ref)

}
