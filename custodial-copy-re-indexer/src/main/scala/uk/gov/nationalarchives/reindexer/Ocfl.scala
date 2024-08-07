package uk.gov.nationalarchives.reindexer

import cats.effect.{Async, Sync}
import cats.effect.kernel.Async
import cats.syntax.all.*
import cats.syntax.all.*
import io.ocfl.api.OcflRepository
import io.ocfl.api.model.OcflObjectVersion
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.reindexer.Main.{Config, FileType}
import uk.gov.nationalarchives.utils.Utils.*
import fs2.io.file.Files
import scala.jdk.CollectionConverters.*
import java.util.UUID

trait Ocfl[F[_]: Sync](config: Config):
  val repo: OcflRepository = createOcflRepository(config.ocflRepoDir, config.ocflWorkDir)
  def readValue(ioIds: List[UUID], fileType: FileType, xpath: String): Unit
object Ocfl:



  enum ReIndexUpdate(id: UUID, columnName: String, value: String):
    case IoUpdate(id: UUID, columnName: String, value: String) extends ReIndexUpdate(id, columnName, value)
    case CoUpdate(id: UUID, columnName: String, value: String) extends ReIndexUpdate(id, columnName, value)

  given impl[F[_]: Sync](using configuration: Configuration): Ocfl[F] = new Ocfl[F](configuration.config):
    override def readValue(ioIds: List[UUID], fileType: FileType, xpath: String): Unit = {
      ioIds.traverse { id =>
        for {
          logger <- Slf4jLogger.create[F]
          ocflObject <- Sync[F].onError(Sync[F].blocking(repo.getObject(id.toHeadVersion)))(err => logger.error(err)(err.getMessage))
          metadataFilePath <- Sync[F].fromOption(ocflObject.getFiles.asScala.find(_.getPath.endsWith(s"${fileType}_Metadata.xml")),
            new Exception(s"${fileType}_Metadata.xml not found for id $id"))
          _ <- Files.
        } yield {

          ()
        }


      }
    }
