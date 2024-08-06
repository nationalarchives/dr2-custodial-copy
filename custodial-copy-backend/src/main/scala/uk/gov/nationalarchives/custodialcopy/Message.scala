package uk.gov.nationalarchives.custodialcopy

import java.util.UUID
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectStatus
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectType

object Message {
  sealed trait ReceivedSnsMessage {
    val ref: UUID
    val messageText: String
  }
  case class IoReceivedSnsMessage(ref: UUID, messageText: String) extends ReceivedSnsMessage
  case class CoReceivedSnsMessage(ref: UUID, messageText: String) extends ReceivedSnsMessage

  case class SendSnsMessage(entityType: EntityType, ioRef: UUID, objectType: ObjectType, status: ObjectStatus, tableItemIdentifier: String | UUID)
}
