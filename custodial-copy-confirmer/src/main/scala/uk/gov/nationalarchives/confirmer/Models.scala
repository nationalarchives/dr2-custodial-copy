package uk.gov.nationalarchives.confirmer

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import io.circe.{Decoder, DecodingFailure, HCursor}
import java.net.URI
import pureconfig.*
import java.util.UUID

case class Config(
    dynamoTableName: String,
    dynamoAttributeName: String,
    sqsUrl: String,
    proxyUrl: URI,
    ocflRepoDir: String,
    ocflWorkDir: String
) derives ConfigReader

extension (s: String) def toAttributeValue: AttributeValue = AttributeValue.builder.s(s).build

case class OutputQueueMessage(assetId: UUID, batchId: String, payload: Payload) {
  def primaryKey: Map[String, AttributeValue] = Map("assetId" -> assetId.toString.toAttributeValue, "batchId" -> batchId.toAttributeValue)
}

trait ConfirmationOperator
final case class CCOperator(ocfl: Ocfl) extends ConfirmationOperator
final case class TCOperator(sam: Any) extends ConfirmationOperator //FIXME: in case operations related to scoutAM need anything

object ConfirmationOperator {
  def getOperator(config: Config, ocfl: Ocfl, sam: Any): ConfirmationOperator =
    config.dynamoAttributeName match {
      case "CC_result" => CCOperator(ocfl)
      case "TC_result" => TCOperator(sam)
      case _           => throw new IllegalArgumentException(s"Unable to create operator corresponding to ${config.dynamoAttributeName}")
    }
}

trait Payload
final case class CCPayload(preservationSystemId: UUID) extends Payload
final case class TCPayload(filePaths: List[String]) extends Payload

object Payload {
  given Decoder[CCPayload] = Decoder.instance { c =>
    for {
      uuid <- c.get[UUID]("preservationSystemId")
    } yield CCPayload(uuid)
  }

  given Decoder[TCPayload] = Decoder.instance { c =>
    c.get[List[String]]("filePaths").map(TCPayload(_))
  }

  given Decoder[Payload] = Decoder.instance { c =>
    if (c.downField("preservationSystemId").succeeded)
      c.as[CCPayload]
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
}

given Decoder[OutputQueueMessage] = (c: HCursor) =>
  for {
    assetId <- c.downField("assetId").as[String]
    batchId <- c.downField("batchId").as[String]
    payload <- c.downField("payload").as[Payload]
  } yield OutputQueueMessage(UUID.fromString(assetId), batchId, payload)
