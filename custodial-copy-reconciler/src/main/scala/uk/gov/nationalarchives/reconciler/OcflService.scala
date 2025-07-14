package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.model.{DigestAlgorithm, OcflObjectVersionFile}
import io.ocfl.api.{MutableOcflRepository, OcflConfig}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.lock.ObjectLockBuilder
import io.ocfl.core.storage.OcflStorageBuilder
import org.h2.jdbcx.JdbcDataSource
import uk.gov.nationalarchives.utils.OcflServiceConfig
import uk.gov.nationalarchives.utils.Utils.*

import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.FunctionConverters.*

trait OcflService {
  def getAllObjectFiles(ioId: UUID): IO[List[OcflObjectVersionFile]]
}
object OcflService {

  def apply(config: OcflServiceConfig): OcflService = {
    val repoDir = Paths.get(config.ocflRepoDir)
    val workDir =
      Paths.get(
        config.ocflWorkDir
      )
    val dataSource = new JdbcDataSource()
    dataSource.setURL(s"jdbc:h2:file:${config.ocflWorkDir}/database")
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
    (ioId: UUID) =>
      IO.blocking(repo.getObject(ioId.toHeadVersion))
        .map(_.getFiles.asScala.toList)
        .handleErrorWith {
          case nfe: NotFoundException => IO.raiseError(new Exception(s"Object id $ioId does not exist"))
          case coe: CorruptObjectException =>
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
}
