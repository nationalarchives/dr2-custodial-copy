package uk.gov.nationalarchives

import cats.effect.IO
import fs2.io.file._

import java.util.UUID
import scala.xml.Elem

sealed trait DisasterRecoveryObject {
  def id: UUID
  def checksum: String
  def name: String
  def path: IO[Path] = createDirectory.map(cd => Path(s"$cd/$name"))
  private def createDirectory: IO[Path] = {
    val path = Path(s"/tmp/${UUID.randomUUID()}/$id")
    Files[IO].createDirectories(path).map(_ => path)
  }
}
object DisasterRecoveryObject {
  case class FileObject(id: UUID, name: String, checksum: String, url: String) extends DisasterRecoveryObject
  case class MetadataObject(id: UUID, name: String, checksum: String, metadata: Elem) extends DisasterRecoveryObject
}
