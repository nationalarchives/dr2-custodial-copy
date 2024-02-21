package uk.gov.nationalarchives

import java.util.UUID

sealed trait Message {
  val ref: UUID
  val messageText: String
}
object Message {
  case class InformationObjectMessage(ref: UUID, messageText: String) extends Message
  case class ContentObjectMessage(ref: UUID, messageText: String) extends Message
}
