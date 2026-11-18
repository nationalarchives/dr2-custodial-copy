package uk.gov.nationalarchives.custodialcopy

import java.util.UUID

object Message {
  sealed trait ReceivedSnsMessage {
    val ref: UUID
    def isCo: Boolean = this match {
      case CoReceivedSnsMessage(_) => true
      case _                       => false
    }
    def isIo: Boolean = this match {
      case IoReceivedSnsMessage(_) => true
      case _                       => false
    }
  }
  case class IoReceivedSnsMessage(ref: UUID) extends ReceivedSnsMessage
  case class CoReceivedSnsMessage(ref: UUID) extends ReceivedSnsMessage
  case class SoReceivedSnsMessage(ref: UUID) extends ReceivedSnsMessage
  case class DeletionReceivedSnsMessage(ref: UUID) extends ReceivedSnsMessage

  case class SendSnsMessage(ioRef: UUID)
}
