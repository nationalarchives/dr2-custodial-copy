package uk.gov.nationalarchives.disasterrecovery

import cats.effect.IO
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId, VersionInfo}
import io.ocfl.api.{OcflConfig, OcflObjectUpdater, OcflOption, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.OcflStorageBuilder
import uk.gov.nationalarchives.disasterrecovery.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.disasterrecovery.OcflService.*

import java.nio.file.Paths
import java.util.UUID
import scala.util.{Failure, Success, Try}
import scala.jdk.FunctionConverters.*

class OcflService(ocflRepository: OcflRepository) {
  def createObjects(paths: List[IdWithSourceAndDestPaths]): IO[List[ObjectVersionId]] = IO.blocking {
    paths
      .groupBy(_.id)
      .view
      .toMap
      .map { case (id, paths) =>
        ocflRepository.updateObject(
          id.toHeadVersion,
          new VersionInfo(),
          { (updater: OcflObjectUpdater) =>
            paths.map { idWithSourceAndDestPath =>
              updater.addPath(
                idWithSourceAndDestPath.sourceNioFilePath,
                idWithSourceAndDestPath.destinationPath,
                OcflOption.OVERWRITE
              )
            }
            ()
          }.asJava
        )
      }
      .toList
  }

  def getMissingAndChangedObjects(
      objects: List[DisasterRecoveryObject]
  ): IO[MissingAndChangedObjects] = IO.blocking {
    val missingAndNonMissingObjects: Map[String, List[DisasterRecoveryObject]] =
      objects.foldLeft(Map[String, List[DisasterRecoveryObject]]("missingObjects" -> Nil, "changedObjects" -> Nil)) { case (objectMap, obj) =>
        val objectId = obj.id
        val potentialOcflObject = Try(ocflRepository.getObject(objectId.toHeadVersion))
        lazy val missedObjects = objectMap("missingObjects")
        lazy val changedObjects = objectMap("changedObjects")

        potentialOcflObject match {
          case Success(ocflObject) =>
            val potentialFile = Option(ocflObject.getFile(obj.destinationFilePath))
            potentialFile match {
              case Some(ocflFileObject) =>
                val checksumUnchanged =
                  Some(ocflFileObject.getFixity.get(DigestAlgorithm.fromOcflName("sha256"))).contains(obj.checksum)
                if checksumUnchanged then objectMap else objectMap + ("changedObjects" -> (obj :: changedObjects))
              case None =>
                objectMap + ("missingObjects" -> (obj :: missedObjects)) // Information Object exists but file doesn't
            }

          case Failure(_: NotFoundException) =>
            objectMap + ("missingObjects" -> (obj :: missedObjects)) // Information Object doesn't exist
          case Failure(coe: CorruptObjectException) =>
            ocflRepository.purgeObject(objectId.toString)
            throw new Exception(
              s"Object $objectId is corrupt. The object has been purged and the error will be rethrown so the process can try again",
              coe
            )
          case Failure(unexpectedError) =>
            throw new Exception(
              s"'getObject' returned an unexpected error '$unexpectedError' when called with object id $objectId"
            )
        }
      }

    MissingAndChangedObjects(
      missingAndNonMissingObjects("missingObjects"),
      missingAndNonMissingObjects("changedObjects")
    )
  }
}
object OcflService {
  extension (uuid: UUID) def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)

  def apply(config: Config): IO[OcflService] = IO {
    val repoDir = Paths.get(config.repoDir)
    val workDir =
      Paths.get(
        config.workDir
      )

    val repo: OcflRepository = new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(((osb: OcflStorageBuilder) => {
        osb.fileSystem(repoDir)
        ()
      }).asJava)
      .ocflConfig(((config: OcflConfig) => {
        config.setDefaultDigestAlgorithm(DigestAlgorithm.fromOcflName("sha256"))
        ()
      }).asJava)
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
