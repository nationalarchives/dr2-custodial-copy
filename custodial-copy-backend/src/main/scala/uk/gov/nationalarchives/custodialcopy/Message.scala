package uk.gov.nationalarchives.custodialcopy

import java.util.UUID
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectStatus
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectType

object Message {
  case class SqsMessage(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)
  
  sealed trait LockTableMessage {
    val ref: UUID
    val deleted: Boolean
  }
  case class IoLockTableMessage(ref: UUID, deleted: Boolean) extends LockTableMessage
  case class CoLockTableMessage(ref: UUID, deleted: Boolean) extends LockTableMessage
  case class SoLockTableMessage(ref: UUID, deleted: Boolean) extends LockTableMessage

  case class SendSnsMessage(entityType: EntityType, ioRef: UUID, objectType: ObjectType, status: ObjectStatus, tableItemIdentifier: String | UUID)
}
