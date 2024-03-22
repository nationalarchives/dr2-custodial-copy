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
import scala.util.{Failure, Success, Try}
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

  def getMissingAndChangedObjects(
      objects: List[DisasterRecoveryObject]
  ): IO[MissingAndChangedObjects] = IO.blocking {
    val missingAndNonMissingObjects: Map[String, List[DisasterRecoveryObject]] =
      objects.foldLeft(Map[String, List[DisasterRecoveryObject]]("missingObjects" -> Nil, "changedObjects" -> Nil)) {
        case (objectMap, obj) =>
          val objectId = obj.id
          val potentialOcflObject = Try(ocflRepository.getObject(objectId.toHeadVersion))
          lazy val missedObjects = objectMap("missingObjects")
          lazy val changedObjects = objectMap("changedObjects")

          potentialOcflObject match {
            case Success(ocflObject) =>
              val checksumUnchanged =
                Option(ocflObject.getFile(s"$objectId/${obj.name}"))
                  .map(_.getFixity.get(DigestAlgorithm.sha256))
                  .contains(obj.checksum)
              if (checksumUnchanged) objectMap else objectMap + ("changedObjects" -> (obj :: changedObjects))

            case Failure(_) => objectMap + ("missingObjects" -> (obj :: missedObjects))
          }
      }

    MissingAndChangedObjects(
      missingAndNonMissingObjects("missingObjects"),
      missingAndNonMissingObjects("changedObjects")
    )
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
      .storage(asJavaConsumer[OcflStorageBuilder](osb => osb.fileSystem(repoDir)))
      .ocflConfig(asJavaConsumer[OcflConfig](config => config.setDefaultDigestAlgorithm(DigestAlgorithm.sha256)))
      .prettyPrintJson()
      .workDir(workDir)
      .build()
    new OcflService(repo)
  }
  case class MissingAndChangedObjects(
      missingObjects: List[DisasterRecoveryObject],
      changedObjects: List[DisasterRecoveryObject]
  )
}
