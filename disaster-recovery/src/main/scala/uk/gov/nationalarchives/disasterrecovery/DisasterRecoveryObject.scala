package uk.gov.nationalarchives.disasterrecovery

import cats.effect.IO
import fs2.io.file.*
import uk.gov.nationalarchives.disasterrecovery.Processor.DependenciesForSnsMsg
import java.util.UUID
import scala.xml.Elem

sealed trait DisasterRecoveryObject {
  val destinationFilePath: String
  def id: UUID
  def checksum: String
  def name: String
  def sourceFilePath: IO[Path] = createDirectory.map(cd => Path(s"$cd/$name"))
  def dependenciesForSnsMsg: DependenciesForSnsMsg
  private def createDirectory: IO[Path] = {
    val path = Path(s"/tmp/${UUID.randomUUID()}/$id")
    Files[IO].createDirectories(path).map(_ => path)
  }
}
object DisasterRecoveryObject {
  case class FileObject(
      id: UUID,
      name: String,
      checksum: String,
      url: String,
      destinationFilePath: String,
      dependenciesForSnsMsg: DependenciesForSnsMsg
  ) extends DisasterRecoveryObject
  case class MetadataObject(
      id: UUID,
      repTypeGroup: Option[String],
      name: String,
      checksum: String,
      metadata: Elem,
      destinationFilePath: String,
      dependenciesForSnsMsg: DependenciesForSnsMsg
  ) extends DisasterRecoveryObject
}
