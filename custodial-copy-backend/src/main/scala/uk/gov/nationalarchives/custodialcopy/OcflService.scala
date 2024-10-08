package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.effect.std.Semaphore
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.model.{DigestAlgorithm, VersionInfo}
import io.ocfl.api.{MutableOcflRepository, OcflConfig, OcflObjectUpdater, OcflOption}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.OcflStorageBuilder
import uk.gov.nationalarchives.custodialcopy.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.custodialcopy.OcflService.*
import uk.gov.nationalarchives.utils.Utils.*

import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.FunctionConverters.*

class OcflService(ocflRepository: MutableOcflRepository, semaphore: Semaphore[IO]) {

  def commitStagedChanges(id: UUID): IO[Unit] =
    IO.whenA(ocflRepository.hasStagedChanges(id.toString)) {
      IO.blocking {
        ocflRepository.commitStagedChanges(id.toString, ocflRepository.getObject(id.toHeadVersion).getVersionInfo)
      }.void
    }

  def createObjects(paths: List[IdWithSourceAndDestPaths]): IO[Unit] = semaphore.acquire >> IO.blocking {
    paths
      .groupBy(_.id)
      .view
      .toMap
      .map { case (id, paths) =>
        ocflRepository.stageChanges(
          id.toHeadVersion,
          new VersionInfo(),
          { (updater: OcflObjectUpdater) =>
            paths.foreach { idWithSourceAndDestPath =>
              updater.addPath(
                idWithSourceAndDestPath.sourceNioFilePath,
                idWithSourceAndDestPath.destinationPath,
                OcflOption.OVERWRITE
              )
            }
          }.asJava
        )
      }
      .toList
  } >> semaphore.release

  def deleteObjects(ioId: UUID, destinationFilePaths: List[String]): IO[Unit] = semaphore.acquire >> IO.blocking {
    ocflRepository.stageChanges(
      ioId.toHeadVersion,
      new VersionInfo(),
      { (updater: OcflObjectUpdater) => destinationFilePaths.foreach { path => updater.removeFile(path) } }.asJava
    )
  } >> semaphore.release

  def getMissingAndChangedObjects(
      objects: List[CustodialCopyObject]
  ): IO[MissingAndChangedObjects] = objects
    .foldLeft(IO(List[CustodialCopyObject](), List[CustodialCopyObject]())) { case (missingAndChangedIO, obj) =>
      val objectId = obj.id
      missingAndChangedIO.flatMap { missingAndChanged =>
        val (missingObjects, changedObjects) = missingAndChanged
        IO.blocking(ocflRepository.getObject(objectId.toHeadVersion))
          .map { ocflObject =>
            val potentialFile = Option(ocflObject.getFile(obj.destinationFilePath))
            potentialFile match {
              case Some(ocflFileObject) =>
                val checksumUnchanged = isChecksumUnchanged(ocflFileObject.getFixity.asScala.toMap, obj.checksums)
                if checksumUnchanged then missingAndChanged else (missingObjects, obj :: changedObjects)
              case None =>
                (obj :: missingObjects, changedObjects) // Information Object exists but file doesn't
            }
          }
          .handleErrorWith {
            case _: NotFoundException => IO.pure(obj :: missingObjects, changedObjects)
            case coe: CorruptObjectException =>
              IO.blocking(ocflRepository.purgeObject(objectId.toString)) >>
                IO.raiseError(
                  new Exception(
                    s"Object $objectId is corrupt. The object has been purged and the error will be rethrown so the process can try again",
                    coe
                  )
                )
            case e: Throwable =>
              IO.raiseError(
                new Exception(
                  s"'getObject' returned an unexpected error '$e' when called with object id $objectId"
                )
              )
          }
      }
    }
    .map { missingAndChangedPair =>
      val (missingObjects, changedObjects) = missingAndChangedPair
      MissingAndChangedObjects(missingObjects, changedObjects)
    }

  private def isChecksumUnchanged(fixitiesMap: Map[DigestAlgorithm, String], checksums: List[Checksum]): Boolean = {
    if (fixitiesMap.size != checksums.size) false
    else checksums.forall(eachChecksum => fixitiesMap.get(DigestAlgorithm.fromOcflName(eachChecksum.algorithm.toLowerCase)).contains(eachChecksum.fingerprint))
  }

  def getAllFilePathsOnAnObject(ioId: UUID): IO[List[String]] =
    IO.blocking(ocflRepository.getObject(ioId.toHeadVersion))
      .map(_.getFiles.asScala.toList.map(_.getPath))
      .handleErrorWith {
        case nfe: NotFoundException => IO.raiseError(new Exception(s"Object id $ioId does not exist"))
        case coe: CorruptObjectException =>
          IO.blocking(ocflRepository.purgeObject(ioId.toString)) >>
            IO.raiseError(
              new Exception(
                s"Object $ioId is corrupt. The object has been purged and the error will be rethrown so the process can try again",
                coe
              )
            )
        case e: Throwable =>
          IO.raiseError(
            new Exception(
              s"'getObject' returned an unexpected error '$e' when called with object id $ioId"
            )
          )
      }
}
object OcflService {

  def apply(config: Config, semaphore: Semaphore[IO]): IO[OcflService] = IO {
    val repoDir = Paths.get(config.repoDir)
    val workDir =
      Paths.get(
        config.workDir
      )

    val repo: MutableOcflRepository = new OcflRepositoryBuilder()
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
      .buildMutable()
    new OcflService(repo, semaphore)
  }
  case class MissingAndChangedObjects(
      missingObjects: List[CustodialCopyObject],
      changedObjects: List[CustodialCopyObject]
  )
}
