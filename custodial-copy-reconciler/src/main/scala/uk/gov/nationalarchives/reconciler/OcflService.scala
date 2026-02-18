package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.effect.implicits.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import io.ocfl.api.model.DigestAlgorithm
import io.ocfl.api.{MutableOcflRepository, OcflConfig}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.lock.ObjectLockBuilder
import io.ocfl.core.storage.OcflStorageBuilder
import org.h2.jdbcx.JdbcDataSource
import uk.gov.nationalarchives.reconciler.Database.CoRow
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.utils.Utils.*

import java.nio.file.Paths
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.FunctionConverters.*

trait OcflService[F[_]] {
  def getAllObjectFiles: Stream[F, CoRow]
}
object OcflService {

  def apply[F[_]: Async](config: Config): OcflService[F] = {
    val startTime = OffsetDateTime.now
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

    def isNotMetadataFile(storageRelativePath: String) =
      (storageRelativePath.contains("/Preservation_") || storageRelativePath.contains("/Access_")) && !storageRelativePath.contains("CO_Metadata.xml")

    def filesForId(id: String) = {
      val ioRef = UUID.fromString(id)
      val obj = repo.getObject(ioRef.toHeadVersion)
      if obj.getCreated.isAfter(startTime.minusDays(config.daysToIgnore)) then Async[F].pure(Chunk.empty)
      else
        val chunk = Chunk.from(obj.getFiles.asScala).collect {
          case coFile if isNotMetadataFile(coFile.getStorageRelativePath) =>
            val pathAsList = coFile.getPath.split("/")
            val pathStartingFromRepType = pathAsList.dropWhile(pathPart => !pathPart.startsWith("Preservation_") && !pathPart.startsWith("Access_"))
            val coRef = UUID.fromString(pathStartingFromRepType(1))
            val fixities = coFile.getFixity.asScala.toMap.map { case (digestAlgo, value) => (digestAlgo.getOcflName, value) }
            val sha256 = fixities("sha256")
            CoRow(coRef, Option(ioRef), sha256)
        }
        Async[F].pure(chunk)
    }

    new OcflService[F] {
      override def getAllObjectFiles: Stream[F, CoRow] =
        Stream
          .fromIterator(repo.listObjectIds().iterator().asScala, config.maxConcurrency)
          .chunkN(50)
          .flatMap(chunk => Stream.evalUnChunk(chunk.parFlatTraverse(filesForId)))
    }
  }
}
