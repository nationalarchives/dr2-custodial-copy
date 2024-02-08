package uk.gov.nationalarchives

import uk.gov.nationalarchives.Message._

import java.util.UUID

sealed trait Message {
  def ref: UUID
  def show: String = this match {
    case _: ContentObjectMessage     => s"CO:$ref"
    case _: InformationObjectMessage => s"IO:$ref"
  }
}
object Message {
  case class InformationObjectMessage(ref: UUID) extends Message
  case class ContentObjectMessage(ref: UUID) extends Message
}
