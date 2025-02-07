package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.model.DigestAlgorithm
import io.ocfl.api.{MutableOcflRepository, OcflConfig, OcflObjectUpdater, OcflOption}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.lock.ObjectLockBuilder
import io.ocfl.core.storage.OcflStorageBuilder
import org.h2.jdbcx.JdbcDataSource
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.custodialcopy.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.custodialcopy.OcflService.*
import uk.gov.nationalarchives.utils.Utils.*

import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.FunctionConverters.*

class OcflService(ocflRepository: MutableOcflRepository, semaphore: Semaphore[IO]) {

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  private def logErrorAndRelease(err: Throwable): IO[Unit] = Logger[IO].error(err)(err.getMessage) >> semaphore.release

  def commitStagedChanges(id: UUID): IO[Unit] =
    semaphore.acquire >> IO.whenA(ocflRepository.hasStagedChanges(id.toString)) {
      IO.blocking[Unit] {
        ocflRepository.commitStagedChanges(id.toString, null)
      }.onError(logErrorAndRelease)
    } >> semaphore.release

  def createObjects(paths: List[IdWithSourceAndDestPaths]): IO[Unit] = paths
    .traverse { path =>
      semaphore.acquire >> IO.blocking {
        ocflRepository.stageChanges(
          path.id.toHeadVersion,
          null,
          { (updater: OcflObjectUpdater) =>
            updater.addPath(
              path.sourceNioFilePath,
              path.destinationPath,
              OcflOption.MOVE_SOURCE,
              OcflOption.OVERWRITE
            )
            ()
          }.asJava
        )
      } >> semaphore.release
    }
    .onError(logErrorAndRelease)
    .void

  def deleteObjects(ioId: UUID, destinationFilePaths: List[String]): IO[Unit] = semaphore.acquire >> IO
    .blocking {
      ocflRepository.stageChanges(
        ioId.toHeadVersion,
        null,
        { (updater: OcflObjectUpdater) => destinationFilePaths.foreach { path => updater.removeFile(path) } }.asJava
      )
    }
    .onError(logErrorAndRelease) >> semaphore.release

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
              purgeObject(objectId) >>
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

  private def purgeObject(objectId: UUID) = {
    semaphore.acquire >> IO.blocking(ocflRepository.purgeObject(objectId.toString)).onError(logErrorAndRelease) >> semaphore.release
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
          purgeObject(ioId) >>
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
    val dataSource = new JdbcDataSource()
    dataSource.setURL(s"jdbc:h2:file:${config.workDir}/database")
    val repo: MutableOcflRepository = new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .objectLock(new ObjectLockBuilder().dataSource(dataSource).build())
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
