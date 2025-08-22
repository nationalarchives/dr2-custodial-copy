package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.effect.implicits.*
import cats.syntax.all.*
import io.ocfl.api.model.DigestAlgorithm
import io.ocfl.api.{MutableOcflRepository, OcflConfig}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.lock.ObjectLockBuilder
import io.ocfl.core.storage.OcflStorageBuilder
import org.h2.jdbcx.JdbcDataSource
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.utils.Utils.*
import fs2.{Chunk, Stream}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.ContentObject

import java.nio.file.Paths
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.FunctionConverters.*

trait OcflService[F[_]] {
  def getAllObjectFiles: Stream[F, OcflCoRow]
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

    def isOriginalAndPreservation(storageRelativePath: String) =
      storageRelativePath.contains("/original/") && storageRelativePath.contains("/Preservation_")

    def filesForId(id: String) = {
      val ioRef = UUID.fromString(id)
      val chunk = Chunk.from(repo.getObject(ioRef.toHeadVersion).getFiles.asScala).collect {
        case coFile if isOriginalAndPreservation(coFile.getStorageRelativePath) =>
          val pathAsList = coFile.getStorageRelativePath.split("/")
          val pathStartingFromRepType = pathAsList.dropWhile(pathPart => !pathPart.startsWith("Preservation_"))
          val coRef = UUID.fromString(pathStartingFromRepType(1))
          val fixities = coFile.getFixity.asScala.toMap.map { case (digestAlgo, value) => (digestAlgo.getOcflName, value) }
          val potentialSha256 = fixities.get("sha256")
          OcflCoRow(coRef, Option(ioRef), ContentObject, potentialSha256, Instant.now.toEpochMilli)
      }
      Async[F].pure(chunk)
    }

    new OcflService[F] {
      override def getAllObjectFiles: Stream[F, OcflCoRow] =
        Stream.fromIterator(repo.listObjectIds().iterator().asScala, config.maxConcurrency)
          .chunkN(50)
          .flatMap(chunk => Stream.evalUnChunk(chunk.parFlatTraverse(filesForId)))
    }

  }
}
