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
  def createObjects(paths: List[IdWithPath]): IO[List[ObjectVersionId]] = IO.blocking {
    paths
      .groupBy(_.id)
      .view
      .mapValues(_.map(_.path))
      .toMap
      .map { case (id, paths) =>
        ocflRepository.updateObject(
          id.toHeadVersion,
          new VersionInfo(),
          asJavaConsumer[OcflObjectUpdater] { updater =>
            paths.map { filePath =>
              updater.addPath(filePath, s"$id/${filePath.getFileName}")
            }
          }
        )
      }
      .toList
  }

  def updateObjects(paths: List[IdWithPath]): IO[List[ObjectVersionId]] = IO.blocking {
    paths.map(path => ocflRepository.putObject(path.id.toHeadVersion, path.path, new VersionInfo()))
  }

  def filterMissingObjects(objects: List[DisasterRecoveryObject]): List[DisasterRecoveryObject] =
    objects.filter { obj =>
      if (ocflRepository.containsObject(obj.id.toString))
        !ocflRepository.getObject(obj.id.toHeadVersion).containsFile(s"${obj.id}/${obj.name}")
      else true
    }

  def filterChangedObjects(objects: List[DisasterRecoveryObject]): List[DisasterRecoveryObject] =
    objects.filter(obj => isChecksumMismatched(obj.id, obj.name, obj.checksum))

  private def isChecksumMismatched(objectId: UUID, fileName: String, checksum: String): Boolean =
    Option
      .when(ocflRepository.containsObject(objectId.toString)) {
        Option(ocflRepository.getObject(objectId.toHeadVersion).getFile(s"$objectId/$fileName"))
          .map(ocflFileObject => ocflFileObject.getFixity.get(DigestAlgorithm.sha256))
          .filterNot(_ == checksum)
          .toList
          .headOption
      }
      .flatten
      .isDefined
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
      .storage(asJavaConsumer[OcflStorageBuilder](osb => osb.fileSystem(repoDir)))
      .ocflConfig(asJavaConsumer[OcflConfig](config => config.setDefaultDigestAlgorithm(DigestAlgorithm.sha256)))
      .prettyPrintJson()
      .workDir(workDir)
      .build()
    new OcflService(repo)
  }
}
