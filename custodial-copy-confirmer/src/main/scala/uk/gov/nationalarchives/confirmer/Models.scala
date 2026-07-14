package uk.gov.nationalarchives.confirmer

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.generic.semiauto.*
import io.circe.parser.parse

import java.net.URI
import pureconfig.*

import java.util.UUID

case class Config(
    dynamoTableName: String,
    dynamoAttributeName: String,
    sqsUrl: String,
    proxyUrl: Option[URI],
    ocflRepoDir: String,
    ocflWorkDir: String,
    scoutamBaseUrl: Option[String] = None,
    scoutamUsername: Option[String] = None,
    scoutamPassword: Option[String] = None
) derives ConfigReader

extension (s: String) def toAttributeValue: AttributeValue = AttributeValue.builder.s(s).build

case class OutputQueueMessage(assetId: UUID, batchId: String, payload: Payload):
  def primaryKey: Map[String, AttributeValue] = Map("assetId" -> assetId.toString.toAttributeValue, "batchId" -> batchId.toAttributeValue)

trait ConfirmationService
final case class CCService(ocfl: Ocfl) extends ConfirmationService
final case class TCService(scoutAM: ScoutAM) extends ConfirmationService

object ConfirmationService:
  def getInstance(config: Config, ocfl: Ocfl, scoutAM: ScoutAM): ConfirmationService =
    ResultAttributeName.fromString(config.dynamoAttributeName) match
      case ResultAttributeName.RESULT_CC => CCService(ocfl)
      case ResultAttributeName.RESULT_TC =>
        config.scoutamUsername.getOrElse(throw new RuntimeException("Unable to authenticate, ScoutAM credentials not found"))
        TCService(scoutAM)

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

enum ResultAttributeName(val value: String):
  case RESULT_TC extends ResultAttributeName("result_TC")
  case RESULT_CC extends ResultAttributeName("result_CC")

  override def toString: String = value

object ResultAttributeName:
  def fromString(value: String): ResultAttributeName = value match
    case "result_TC" => ResultAttributeName.RESULT_TC
    case "result_CC" => ResultAttributeName.RESULT_CC
    case _           => throw new IllegalArgumentException(s"Unknown ResultAttributeName: $value")
