package uk.gov.nationalarchives.confirmer

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import io.circe.Decoder
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.confirmer.Main.{Config, Message}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.InformationObject
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier
import uk.gov.nationalarchives.utils.TestUtils.TestSqsClient
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}

import java.net.URI
import java.nio.file.Files
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

object TestUtils:
  def createError[T](msg: String): IO[T] = IO.raiseError(new Exception(msg))

  def notImplemented[T]: IO[T] = createError("Not implemented")

  case class Errors(dynamoUpdateError: Boolean = false, sqsReceiveError: Boolean = false, sqsDeleteError: Boolean = false) {
    def hasReceiveMessagesError: IO[Unit] = IO.whenA(sqsReceiveError)(createError[Unit]("Error receiving messages"))

    def hasDynamoUpdateError: IO[Unit] = IO.whenA(dynamoUpdateError)(createError[Unit]("Error updating Dynamo table"))

    def hasDeleteError: IO[Unit] = IO.whenA(sqsDeleteError)(createError[Unit]("Error deleting from SQS"))
  }

  def runConfirmer(
      messages: List[MessageResponse[Message]],
      existingRefs: List[UUID],
      assetIdToEntityIds: Map[UUID, List[UUID]],
      errors: Errors,
      allowMultipleSqsCalls: Boolean = false
  ): (List[String], List[DADynamoDbRequest]) = (for {
    messagesRef <- Ref.of[IO, List[MessageResponse[Message]]](messages)
    dynamoRef <- Ref.of[IO, List[DADynamoDbRequest]](Nil)
    deletedMessagesRef <- Ref.of[IO, List[String]](Nil)
    workDir = Files.createTempDirectory("work")
    repoDir = Files.createTempDirectory("repo")
    config = Config("table", "attribute", "", URI.create("https://example.com"), repoDir.toString, workDir.toString, "", "")
    _ <- Main
      .runConfirmer(
        config,
        daSqsClient(messagesRef, deletedMessagesRef, errors, allowMultipleSqsCalls),
        daDynamoDbClient(dynamoRef, errors),
        ocfl(existingRefs, config),
        preservicaClient(assetIdToEntityIds)
      )
      .compile
      .drain
    messages <- deletedMessagesRef.get
    dynamoRequests <- dynamoRef.get
  } yield (messages, dynamoRequests)).unsafeRunSync()

  def preservicaClient(assetIdToEntityIds: Map[UUID, List[UUID]]) = new EntityClient[IO, Fs2Streams[IO]] {

    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

    override def metadataForEntity(entity: Entities.Entity): IO[EntityClient.EntityMetadata] = notImplemented

    override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = notImplemented

    override def getEntity(entityRef: UUID, entityType: EntityClient.EntityType): IO[Entities.Entity] = notImplemented

    override def getEntityIdentifiers(entity: Entities.Entity): IO[Seq[IdentifierResponse]] = notImplemented

    override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = notImplemented

    override def getContentObjectsFromRepresentation(
        ioEntityRef: UUID,
        representationType: EntityClient.RepresentationType,
        repTypeIndex: Int
    ): IO[Seq[Entities.Entity]] = notImplemented

    override def addEntity(addEntityRequest: EntityClient.AddEntityRequest): IO[UUID] = notImplemented

    override def updateEntity(updateEntityRequest: EntityClient.UpdateEntityRequest): IO[String] = notImplemented

    override def updateEntityIdentifiers(entity: Entities.Entity, identifiers: Seq[Entities.IdentifierResponse]): IO[Seq[Entities.IdentifierResponse]] =
      notImplemented

    override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] =
      notImplemented

    override def entitiesUpdatedSince(dateTime: ZonedDateTime, startEntry: Int, maxEntries: Int): IO[Seq[Entities.Entity]] = notImplemented

    override def entityEventActions(entity: Entities.Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = notImplemented

    override def entitiesPerIdentifier(identifiers: Seq[Identifier]): IO[Map[Identifier, Seq[Entities.Entity]]] = IO {
      identifiers
        .map(identifier => identifier -> assetIdToEntityIds.get(UUID.fromString(identifier.value)).toList.flatten)
        .toMap
        .map { case (identifier, entityIds) =>
          identifier -> entityIds.map(entityId => Entity(Option(InformationObject), entityId, None, None, false, None))
        }
    }

    override def addIdentifierForEntity(entityRef: UUID, entityType: EntityClient.EntityType, identifier: Identifier): IO[String] = notImplemented

    override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = notImplemented
  }

  def daSqsClient(
      ref: Ref[IO, List[MessageResponse[Message]]],
      deletedMessagesRef: Ref[IO, List[String]],
      errors: Errors,
      allowMultiplSqsCalls: Boolean
  ): DASQSClient[IO] = new TestSqsClient {
    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[MessageResponse[T]]] =
      errors.hasReceiveMessagesError >>
        (if allowMultiplSqsCalls then ref.get.asInstanceOf[IO[List[MessageResponse[T]]]]
         else ref.getAndUpdate(_ => Nil).asInstanceOf[IO[List[MessageResponse[T]]]])

    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = errors.hasDeleteError >> deletedMessagesRef
      .update { deletedMessagesList =>
        receiptHandle :: deletedMessagesList
      }
      .map(_ => DeleteMessageResponse.builder.build)
  }

  def daDynamoDbClient(ref: Ref[IO, List[DADynamoDbRequest]], errors: Errors): DADynamoDBClient[IO] = new DADynamoDBClient[IO] {
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using
        returnTypeFormat: DynamoFormat[U]
    ): IO[List[U]] = notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      notImplemented

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = errors.hasDynamoUpdateError >> ref
      .update { requests =>
        dynamoDbRequest :: requests
      }
      .map(_ => 1)
  }

  def ocfl(existingRefs: List[UUID], config: Config): Ocfl = new Ocfl(config) {
    override def checkObjectExists(id: UUID): Boolean = existingRefs.contains(id)
  }
