package uk.gov.nationalarchives.disasterrecovery

import java.util.UUID
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType
import uk.gov.nationalarchives.disasterrecovery.Processor.ObjectStatus
import uk.gov.nationalarchives.disasterrecovery.Processor.ObjectType

object Message {
  sealed trait ReceivedSnsMessage {
    val ref: UUID
    val messageText: String
  }

  case class InformationObjectReceivedSnsMessage(ref: UUID, messageText: String) extends ReceivedSnsMessage
  case class ContentObjectReceivedSnsMessage(ref: UUID, messageText: String) extends ReceivedSnsMessage

  case class SendSnsMessage(entityType: EntityType, ioRef: UUID, objectType: ObjectType, status: ObjectStatus, tableItemIdentifier: String | UUID)
}
