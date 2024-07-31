package uk.gov.nationalarchives.disasterrecovery

import java.util.UUID
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType
import uk.gov.nationalarchives.disasterrecovery.Processor.ObjectStatus
import uk.gov.nationalarchives.disasterrecovery.Processor.ObjectType

object Message {
  sealed trait ReceivedSnsMessage {
    val ref: UUID
    val messageText: String
    val deleted: Boolean
  }
  case class IoReceivedSnsMessage(ref: UUID, messageText: String, deleted: Boolean) extends ReceivedSnsMessage
  case class CoReceivedSnsMessage(ref: UUID, messageText: String, deleted: Boolean) extends ReceivedSnsMessage

  case class SendSnsMessage(entityType: EntityType, ioRef: UUID, objectType: ObjectType, status: ObjectStatus, tableItemIdentifier: String | UUID)
}
