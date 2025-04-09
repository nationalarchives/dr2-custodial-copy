package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import fs2.io.file.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType

import java.util.UUID
import scala.xml.Elem

sealed trait CustodialCopyObject {
  val destinationFilePath: String
  def id: UUID
  def checksums: List[Checksum]
  def name: String
  def sourceFilePath(workDir: String): IO[Path] = createDirectory(workDir).map(dir => Path(s"$dir/$name"))
  def tableItemIdentifier: String | UUID
  private def createDirectory(workDir: String): IO[Path] = {
    val path = Path(s"$workDir/downloads/${UUID.randomUUID()}/$id")
    Files[IO].createDirectories(path).map(_ => path)
  }
}
object CustodialCopyObject {
  case class FileObject(
      id: UUID,
      name: String,
      checksums: List[Checksum],
      url: String,
      destinationFilePath: String,
      tableItemIdentifier: UUID
  ) extends CustodialCopyObject
  case class MetadataObject(
      id: UUID,
      entityType: EntityType,
      repTypeGroup: Option[String],
      name: String,
      checksums: List[Checksum],
      metadata: Elem,
      destinationFilePath: String,
      tableItemIdentifier: String | UUID
  ) extends CustodialCopyObject
}

case class Checksum(algorithm: String, fingerprint: String)
