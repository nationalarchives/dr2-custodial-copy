package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.syntax.all.*
import io.ocfl.api.exception.NotFoundException
import io.ocfl.api.model.{DigestAlgorithm, OcflObjectVersionFile}
import io.ocfl.api.{MutableOcflRepository, OcflConfig}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.lock.ObjectLockBuilder
import io.ocfl.core.storage.OcflStorageBuilder
import org.h2.jdbcx.JdbcDataSource
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.utils.Utils.*

import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.FunctionConverters.*

trait OcflService[F[_]] {
  def getAllObjectFiles(ioId: UUID): F[List[OcflObjectVersionFile]]
}
object OcflService {

  def apply[F[_]: Async](config: Config): OcflService[F] = {
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
      Async[F]
        .blocking(repo.getObject(ioId.toHeadVersion))
        .map(_.getFiles.asScala.toList)
        .handleErrorWith { case nfe: NotFoundException => Async[F].pure(Nil) }
  }
}
