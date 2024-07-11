package uk.gov.nationalarchives.utils

import doobie.util.{Get, Put}

import java.time.Instant
import java.util.UUID
object Utils:

  given Get[UUID] = Get[String].map(UUID.fromString)

  given Put[UUID] = Put[String].contramap(_.toString)

  given Get[Instant] = Get[Long].map(Instant.ofEpochMilli)

  given Put[Instant] = Put[Long].contramap(_.toEpochMilli)

  final case class OcflFile(
      versionNum: Long,
      id: UUID,
      name: Option[String],
      fileId: UUID,
      zref: Option[String],
      path: Option[String],
      fileName: Option[String],
      ingestDateTime: Option[Instant],
      sourceId: Option[String],
      citation: Option[String]
  )
