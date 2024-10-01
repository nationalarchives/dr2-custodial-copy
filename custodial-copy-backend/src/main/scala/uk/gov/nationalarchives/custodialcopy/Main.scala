package uk.gov.nationalarchives.custodialcopy

import cats.effect.*
import cats.effect.kernel.Outcome.*
import cats.effect.std.Semaphore
import cats.implicits.*
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import uk.gov.nationalarchives.custodialcopy.Message.*
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient, DASQSClient}

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
      topicArn: String,
      lockTableName: String,
      lockTableGsiName: String
  ) derives ConfigReader

  private def sqsClient(config: Config): DASQSClient[IO] =
    config.proxyUrl
      .map(proxy => DASQSClient[IO](proxy))
      .getOrElse(DASQSClient[IO]())

  private def dynamoService(config: Config): DynamoService =
    val dynamoDbClient = config.proxyUrl
      .map(proxy => DADynamoDBClient[IO](proxy))
      .getOrElse(DADynamoDBClient [IO]())
    DynamoService(dynamoDbClient, config)

  given Encoder[SqsMessage] = deriveEncoder[SqsMessage]

  given Decoder[SqsMessage] = deriveDecoder[SqsMessage]

  given Decoder[LockTableMessage] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[String]
      deleted <- c.downField("deleted").as[Boolean]
    } yield {
      val typeAndRef = id.split(":")
      val ref = UUID.fromString(typeAndRef.last)
      val entityType = typeAndRef.head
      entityType match {
        case "io" => IoLockTableMessage(ref, deleted)
        case "co" => CoLockTableMessage(ref, deleted)
        case "so" => SoLockTableMessage(ref, deleted)
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
      dynamo = dynamoService(config)
      sqs = sqsClient(config)
      sns = DASNSClient[IO]()
      processor <- Processor(config, service, client, sns)
      _ <- {
        Stream.fixedRateStartImmediately[IO](10.seconds) >>
          runCustodialCopy(sqs, dynamo, config, processor)
            .handleErrorWith(err => Stream.eval(semaphore.release >> logError(err)))
      }.compile.drain
    } yield ExitCode.Success

  def runCustodialCopy(sqs: DASQSClient[IO], dynamoService: DynamoService, config: Config, processor: Processor): Stream[IO, List[Outcome[IO, Throwable, UUID]]] =
    def sendWithRetryIncremented(inputSqsMessage: SqsMessage): IO[Unit] =
      sqs.sendMessage(config.sqsQueueUrl)(inputSqsMessage.copy(retryCount = inputSqsMessage.retryCount + 1)).void

    Stream
      .eval(sqs.receiveMessages[SqsMessage](config.sqsQueueUrl))
      .filter(messageResponses => messageResponses.nonEmpty)
      .evalMap { messageResponses =>
        messageResponses
          .traverse { response =>
            for {
              items <- dynamoService.retrieveItems(response.message.groupId)
              allFiberResults <- items.groupBy(_.assetId).map {
                case (assetId, lockTableItems) => processMessages(processor, lockTableItems.map(_.message), assetId.toString)
              }.toList.sequence.map(_.flatten)
              successfulIds <- allFiberResults.collect {
                case Succeeded(ioId) => ioId
              }.sequence
              _ <- dynamoService.deleteItems(successfulIds)
              allFiberErrors = allFiberResults.filter(_.isError)
              _ <- if allFiberErrors.nonEmpty && response.message.retryCount >= 3 then
                    IO.raiseError(new Exception(s"Message for groupId ${response.message.groupId} has been retried 3 times and there are still ${allFiberErrors.size} failures"))
                  else if allFiberErrors.nonEmpty && response.message.retryCount < 3 then
                    sqs.deleteMessage(config.sqsQueueUrl, response.receiptHandle).void >> sendWithRetryIncremented(response.message)
                  else sqs.deleteMessage(config.sqsQueueUrl, response.receiptHandle).void
            } yield allFiberResults
          }
      }.map(_.flatten)

  private def processMessages(processor: Processor, lockTableMessages: List[LockTableMessage], groupId: String): IO[List[Outcome[IO, Throwable, UUID]]] = {
    for {
      logger <- Slf4jLogger.create[IO]
      _ <- logger.info(s"Processing ${lockTableMessages.length} messages")
      dedupedMessages = dedupeMessages(lockTableMessages)
      (deletedEntities, nonDeletedEntities) = dedupedMessages.partition(_.deleted)

      fibersForDeletedEntities <- deletedEntities.traverse(processor.process(_, true).start)

      fibersForNonDeletedEntities <- nonDeletedEntities.parTraverse(processor.process(_, false).start)
      ndeFiberResults <- fibersForNonDeletedEntities.traverse(_.join)

      fibersForDeletedEntities <- deletedEntities.parTraverse(processor.process(_, true).start)
      deFiberResults <- fibersForDeletedEntities.traverse(_.join)

      allFibers = fibersForNonDeletedEntities ++ fibersForDeletedEntities
      allFiberResults = ndeFiberResults ++ deFiberResults

      _ <- processor.commitStagedChanges(UUID.fromString(groupId))
      _ <- logger.info(s"${allFiberResults.count(_.isSuccess)} messages out of ${allFibers.length} unique messages processed successfully")
      _ <- logger.info(s"${allFiberResults.count(_.isError)} messages out of ${allFibers.length} unique messages failed")
    } yield allFiberResults
  }

  private def dedupeMessages(messages: List[LockTableMessage]): List[LockTableMessage] =
    messages.distinctBy(_.ref)

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  } yield ()
}
