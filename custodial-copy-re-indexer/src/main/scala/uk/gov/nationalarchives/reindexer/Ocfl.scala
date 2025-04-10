package uk.gov.nationalarchives.reindexer

import cats.effect.Sync
import cats.syntax.all.*
import io.ocfl.api.OcflRepository
import io.ocfl.api.model.OcflObjectVersionFile
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.w3c.dom.Document
import uk.gov.nationalarchives.reindexer.Configuration.{EntityType, ReIndexUpdate}
import uk.gov.nationalarchives.utils.Utils.*

import java.io.File
import java.util.UUID
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathExpression
import scala.jdk.CollectionConverters.*

trait Ocfl[F[_]: Sync]:

  def readValue(ioId: UUID, fileType: EntityType, xpath: XPathExpression)(using configuration: Configuration): F[List[ReIndexUpdate]]
object Ocfl:

  private def fileToXml[F[_]: Sync](path: String): F[Document] = Sync[F].blocking {
    val xmlFile = new File(path)
    val builderFactory = DocumentBuilderFactory.newInstance()
    val builder = builderFactory.newDocumentBuilder()
    builder.parse(xmlFile)
  }

  def apply[F[_]: Sync](using ev: Ocfl[F]): Ocfl[F] = ev

  given impl[F[_]: Sync]: Ocfl[F] = new Ocfl[F]:

    def repo(configuration: Configuration): OcflRepository = createOcflRepository(configuration.config.ocflRepoDir, configuration.config.ocflWorkDir)

    override def readValue(ioId: UUID, fileType: EntityType, xpath: XPathExpression)(using configuration: Configuration): F[List[ReIndexUpdate]] =
      def isValidForIndexing(ocflFile: OcflObjectVersionFile): F[Boolean] =
        val logger = Slf4jLogger.getLogger[F]
        val fullPath = s"${configuration.config.ocflRepoDir}/${ocflFile.getStorageRelativePath}"
        val file = new File(fullPath)

        val isMetadataFile = ocflFile.getPath.endsWith(s"${fileType}_Metadata.xml")
        val isValid = isMetadataFile && file.length == 0

        Sync[F].whenA(isValid)(logger.info(s"Empty file found: $fullPath")).map(_ => isValid) // this may happen only on uat

      for {
        logger <- Slf4jLogger.create[F]
        ocflObject <- Sync[F].onError(Sync[F].blocking(repo(configuration).getObject(ioId.toHeadVersion)))(err => logger.error(err)(err.getMessage))
        objectVersionFiles <- ocflObject.getFiles.asScala.toList.filterA(ocflFile => isValidForIndexing(ocflFile))
        xmlFiles <- objectVersionFiles.traverse(objectVersionFile =>
          fileToXml(s"${configuration.config.ocflRepoDir}/${objectVersionFile.getStorageRelativePath}")
        )
      } yield xmlFiles.map(xmlFile => fileType.getReindexUpdate(xpath, xmlFile))
