package uk.gov.nationalarchives.confirmer

import io.circe.generic.semiauto.*
import io.circe.parser.parse
import io.circe.{Decoder, DecodingFailure, HCursor}
import pureconfig.*
import pureconfig.ConfigReader.Result
import pureconfig.error.ConfigReaderFailures
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.net.URI
import java.util.UUID

given ConfigReader[Config] with
  override def from(cur: ConfigCursor): ConfigReader.Result[Config] =
    for
      obj <- cur.asObjectCursor
      attrCur <- obj.atKey("dynamo-attribute-name")
      attributeName <- attrCur.asString
      decoded <- ResultAttributeName.valueOf(attributeName) match {
        case ResultAttributeName.result_TC => summon[ConfigReader[TCConfig]].from(cur)
        case ResultAttributeName.result_CC => summon[ConfigReader[CCConfig]].from(cur)
      }
    yield decoded

sealed trait Config:
  def dynamoTableName: String
  def dynamoAttributeName: String
  def sqsUrl: String
  def proxyUrl: Option[URI]

case class CCConfig(
    dynamoTableName: String,
    dynamoAttributeName: String,
    sqsUrl: String,
    proxyUrl: Option[URI],
    ocflRepoDir: String,
    ocflWorkDir: String
) extends Config derives ConfigReader

case class TCConfig(
    dynamoTableName: String,
    dynamoAttributeName: String,
    sqsUrl: String,
    proxyUrl: Option[URI],
    scoutamBaseUrl: String,
    scoutamUsername: String,
    scoutamPassword: String
) extends Config derives ConfigReader

extension (s: String) def toAttributeValue: AttributeValue = AttributeValue.builder.s(s).build

case class OutputQueueMessage(assetId: UUID, batchId: String, payload: Payload):
  def primaryKey: Map[String, AttributeValue] = Map("assetId" -> assetId.toString.toAttributeValue, "batchId" -> batchId.toAttributeValue)

trait Payload
final case class CCPayload(preservationSystemId: UUID) extends Payload:
  override def toString = s"CCPayload(preservationSystemId=$preservationSystemId)"

final case class TCPayload(filePaths: List[String]) extends Payload:
  override def toString = s"TCPayload(filePaths=${filePaths.mkString("[", ", ", "]")})"

object Payload:
  given Decoder[CCPayload] = Decoder.instance { c =>
    c.get[UUID]("preservationSystemId").map(CCPayload(_))
  }

  given Decoder[TCPayload] = Decoder.instance { c =>
    c.get[List[String]]("filePaths").map(TCPayload(_))
  }

  given Decoder[Payload] = Decoder.instance { c =>
    if c.downField("preservationSystemId").succeeded then c.as[CCPayload]
    else if (c.downField("filePaths").succeeded)
      c.as[TCPayload]
    else
      Left(
        DecodingFailure(
          "Could not determine payload type. Expected either 'preservationSystemId' or 'filePaths'.",
          c.history
        )
      )
  }

given Decoder[OutputQueueMessage] = (c: HCursor) =>
  for {
    assetId <- c.downField("assetId").as[String]
    batchId <- c.downField("batchId").as[String]
    payloadString <- c.downField("payload").as[String]
    payload <- parse(payloadString).left
      .map(err => io.circe.DecodingFailure(err.message, c.history))
      .flatMap(_.as[Payload])
  } yield OutputQueueMessage(UUID.fromString(assetId), batchId, payload)

final case class FileResponse(archdone: Boolean, copies: List[Copy], checksum: Option[String])
final case class Copy(copy: String, sections: Option[List[Section]])
final case class Section(volume: String)

object FileResponse:
  given Decoder[Section] = deriveDecoder
  given Decoder[Copy] = deriveDecoder
  given Decoder[FileResponse] = deriveDecoder

final case class AuthorisationResponse(token: String)

object AuthorisationResponse:
  given Decoder[AuthorisationResponse] = Decoder.instance { c =>
    c.get[String]("response").map(token => AuthorisationResponse(token))
  }

enum ResultAttributeName:
  case result_TC, result_CC
