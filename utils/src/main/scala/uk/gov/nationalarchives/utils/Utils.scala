package uk.gov.nationalarchives.utils

import java.util.UUID
import doobie.util.{Get, Put}
object Utils:

  given Get[UUID] = Get[String].map(UUID.fromString)

  given Put[UUID] = Put[String].contramap(_.toString)

  final case class OcflFile(versionNum: Long, id: UUID, name: String, fileId: UUID, zref: String, path: String, fileName: String)
