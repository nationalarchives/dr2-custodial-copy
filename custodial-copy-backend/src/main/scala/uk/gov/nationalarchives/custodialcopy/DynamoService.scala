package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import org.scanamo.{DynamoFormat, DynamoReadError, DynamoValue, MissingProperty, TypeCoercionError}
import uk.gov.nationalarchives.DADynamoDBClient
import uk.gov.nationalarchives.custodialcopy.Main.given
import uk.gov.nationalarchives.custodialcopy.Main.Config
import org.scanamo.syntax.*
import io.circe.parser.decode
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.custodialcopy.Message.LockTableMessage
import org.scanamo.generic.semiauto.deriveDynamoFormat
import uk.gov.nationalarchives.custodialcopy.DynamoService.{given, *}
import java.util.UUID
import scala.jdk.CollectionConverters.*

class DynamoService(dynamoDbClient: DADynamoDBClient[IO], config: Config):
  
  def retrieveItems(groupId: String): IO[List[LockTableItem]] =
    dynamoDbClient
      .queryItems[LockTableItem](config.lockTableName, "groupId" === groupId, Option(config.lockTableGsiName))

  def deleteItems(assetIds: List[UUID]): IO[Unit] =
    dynamoDbClient.deleteItems(config.lockTableName, assetIds.map(LockTablePartitionKey.apply)).void
    
object DynamoService:
  case class LockTableItem(assetId: UUID, message: LockTableMessage)

  case class LockTablePartitionKey(assetId: UUID)

  given DynamoFormat[LockTablePartitionKey] = deriveDynamoFormat[LockTablePartitionKey]

  given DynamoFormat[LockTableItem] = new DynamoFormat[LockTableItem]:
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, LockTableItem] = {
      val attributeMap = dynamoValue.toAttributeValue.m().asScala.toMap
      for {
        messageAsString <- attributeMap.get("message").map(_.s()).toRight(MissingProperty)
        assetId <- attributeMap.get("assetId").map(_.s()).map(UUID.fromString).toRight(MissingProperty)
        lockTableMessage <- decode[LockTableMessage](messageAsString).left.map(err => TypeCoercionError(err))
      } yield LockTableItem(assetId, lockTableMessage)
    }

    override def write(t: LockTableItem): DynamoValue = DynamoValue.nil // We're not writing to the lock table here
