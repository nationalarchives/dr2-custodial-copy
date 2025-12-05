package uk.gov.nationalarchives.confirmer

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import io.circe.Decoder
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.{BatchWriteItemResponse, ConditionalCheckFailedException}
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.confirmer.Main.{Config, OutputQueueMessage}
import uk.gov.nationalarchives.utils.TestUtils.TestSqsClient
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}

import java.net.URI
import java.nio.file.Files
import java.util.UUID

object TestUtils:
  def createError[T](msg: String): IO[T] = IO.raiseError(new Exception(msg))

  def notImplemented[T]: IO[T] = createError("Not implemented")

  case class Errors(
      dynamoUpdateError: Boolean = false,
      sqsReceiveError: Boolean = false,
      sqsDeleteError: Boolean = false,
      conditionalCheckError: Boolean = false
  ) {
    def hasReceiveMessagesError: IO[Unit] = IO.whenA(sqsReceiveError)(createError[Unit]("Error receiving messages"))

    def hasDynamoUpdateError: IO[Unit] = IO.whenA(dynamoUpdateError)(createError[Unit]("Error updating Dynamo table"))

    def hasDynamoConditionalCheckError: IO[Unit] = IO.whenA(conditionalCheckError)(IO.raiseError(ConditionalCheckFailedException.builder.build))

    def hasDeleteError: IO[Unit] = IO.whenA(sqsDeleteError)(createError[Unit]("Error deleting from SQS"))
  }

  def runConfirmer(
      messages: List[MessageResponse[OutputQueueMessage]],
      existingRefs: List[UUID],
      errors: Errors,
      allowMultipleSqsCalls: Boolean = false
  ): (List[String], List[DADynamoDbRequest]) = (for {
    messagesRef <- Ref.of[IO, List[MessageResponse[OutputQueueMessage]]](messages)
    dynamoRef <- Ref.of[IO, List[DADynamoDbRequest]](Nil)
    deletedMessagesRef <- Ref.of[IO, List[String]](Nil)
    workDir = Files.createTempDirectory("work")
    repoDir = Files.createTempDirectory("repo")
    config = Config("table", "attribute", "", URI.create("https://example.com"), repoDir.toString, workDir.toString)
    _ <- Main
      .runConfirmer(
        config,
        daSqsClient(messagesRef, deletedMessagesRef, errors, allowMultipleSqsCalls),
        daDynamoDbClient(dynamoRef, errors),
        ocfl(existingRefs, config)
      )
      .compile
      .drain
    messages <- deletedMessagesRef.get
    dynamoRequests <- dynamoRef.get
  } yield (messages, dynamoRequests)).unsafeRunSync()

  def daSqsClient(
      ref: Ref[IO, List[MessageResponse[OutputQueueMessage]]],
      deletedMessagesRef: Ref[IO, List[String]],
      errors: Errors,
      allowMultipleSqsCalls: Boolean
  ): DASQSClient[IO] = new TestSqsClient {
    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[MessageResponse[T]]] =
      errors.hasReceiveMessagesError >>
        (if allowMultipleSqsCalls then ref.get.asInstanceOf[IO[List[MessageResponse[T]]]]
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

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] =
      errors.hasDynamoUpdateError >> errors.hasDynamoConditionalCheckError >> ref
        .update { requests =>
          dynamoDbRequest :: requests
        }
        .map(_ => 1)
  }

  def ocfl(existingRefs: List[UUID], config: Config): Ocfl = new Ocfl(config) {
    override def checkObjectExists(id: UUID): Boolean = existingRefs.contains(id)
  }
