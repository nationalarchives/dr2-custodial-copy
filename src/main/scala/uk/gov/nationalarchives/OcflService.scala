package uk.gov.nationalarchives

import cats.effect.IO
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId, VersionInfo}
import io.ocfl.api.{OcflConfig, OcflObjectUpdater, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.OcflStorageBuilder
import uk.gov.nationalarchives.Main.{Config, IdWithPath}
import uk.gov.nationalarchives.OcflService._
import java.nio.file.Paths
import java.util.UUID
import scala.compat.java8.FunctionConverters._

class OcflService(ocflRepository: OcflRepository) {
  def writeObjects(paths: List[IdWithPath]): IO[List[ObjectVersionId]] = IO {
    if (paths.isEmpty) {
      Nil
    } else {
      paths
        .groupBy(_.id)
        .view
        .mapValues(_.map(_.path))
        .toMap
        .map { case (id, paths) =>
          ocflRepository.updateObject(
            id.toHeadVersion,
            new VersionInfo(),
            asJavaConsumer[OcflObjectUpdater](updater => {
              paths.map(filePath => {
                updater.addPath(filePath, s"$id/${filePath.getFileName}")
              })
            })
          )
        }
        .toList
    }
  }

  def updateObjects(paths: List[IdWithPath]): IO[List[ObjectVersionId]] = IO {
    paths.map(path => ocflRepository.putObject(path.id.toHeadVersion, path.path, new VersionInfo()))
  }

  def filterMissingItems[T <: DisasterRecoveryItem](items: List[T]): List[T] = {
    items.filter { item =>
      if (ocflRepository.containsObject(item.id.toString))
        !ocflRepository.getObject(item.id.toHeadVersion).containsFile(s"${item.id}/${item.name}")
      else true
    }
  }

  def filterChangedItems[T <: DisasterRecoveryItem](items: List[T]): List[T] = {
    items.filter(item => isChecksumMismatched(item.id, item.name, item.checksum))
  }

  private def isChecksumMismatched(ioId: UUID, fileName: String, checksum: String): Boolean = {
    Option
      .when(ocflRepository.containsObject(ioId.toString)) {
        Option(ocflRepository.getObject(ioId.toHeadVersion).getFile(s"$ioId/$fileName"))
          .map(_.getFixity.get(DigestAlgorithm.sha256))
          .filterNot(_ == checksum)
          .toList
          .headOption
      }
      .flatten
      .isDefined
  }

}
object OcflService {
  implicit class UuidUtils(uuid: UUID) {
    def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)
  }

  def apply(config: Config): IO[OcflService] = IO {
    val repoDir = Paths.get(config.repoDir)
    val workDir =
      Paths.get(
        config.workDir
      )

    val repo: OcflRepository = new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(asJavaConsumer[OcflStorageBuilder](s => s.fileSystem(repoDir)))
      .ocflConfig(asJavaConsumer[OcflConfig](config => config.setDefaultDigestAlgorithm(DigestAlgorithm.sha256)))
      .prettyPrintJson()
      .workDir(workDir)
      .build()
    new OcflService(repo)
  }
}
