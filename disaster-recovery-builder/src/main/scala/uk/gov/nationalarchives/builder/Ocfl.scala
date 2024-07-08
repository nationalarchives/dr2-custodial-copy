package uk.gov.nationalarchives.builder

import cats.effect.kernel.Async
import cats.implicits.*
import uk.gov.nationalarchives.builder.Main.Config
import uk.gov.nationalarchives.utils.Utils.OcflFile
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId, OcflObjectVersionFile}
import io.ocfl.api.{OcflConfig, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.{OcflStorage, OcflStorageBuilder}

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.xml.XML

trait Ocfl[F[_]](val config: Config):
  private[builder] val repo: OcflRepository = {
    val repoDir = Paths.get(config.ocflRepoDir)
    val workDir = Paths.get(config.ocflWorkDir)
    val storage: OcflStorage = OcflStorageBuilder.builder().fileSystem(repoDir).build
    val ocflConfig: OcflConfig = new OcflConfig()
    ocflConfig.setDefaultDigestAlgorithm(DigestAlgorithm.sha256)
    new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(storage)
      .ocflConfig(ocflConfig)
      .prettyPrintJson()
      .workDir(workDir)
      .build()
  }

  def generate(id: UUID): F[List[OcflFile]]

object Ocfl:
  def apply[F[_]](using ev: Ocfl[F]): Ocfl[F] = ev

  given impl[F[_]: Async](using configuration: Configuration): Ocfl[F] = new Ocfl[F](configuration.config):
    override def generate(id: UUID): F[List[OcflFile]] = Async[F].pure {

      val objectVersion = repo.getObject(ObjectVersionId.head(id.toString))
      val files = objectVersion.getFiles.asScala.toList

      def loadMetadataXml(file: OcflObjectVersionFile) = XML.loadString(Files.readString(Paths.get(config.ocflRepoDir, file.getStorageRelativePath)))

      val ioMetadata = files.find(_.getPath.contains(s"IO_Metadata.xml")).map(loadMetadataXml)
      val zref = ioMetadata
        .flatMap { metadata =>
          (metadata \ "Identifiers" \ "Identifier").toList
            .find(i => (i \ "Type").text == "BornDigitalRef")
            .map(i => (i \ "Value").text)
        }
        .getOrElse("")

      val title = ioMetadata
        .map { metadata =>
          (metadata \ "InformationObject" \ "Title").text
        }
        .getOrElse("")

      files.filter(_.getPath.contains(s"CO_Metadata.xml")).map { coMetadataFile =>
        val coMetadataPath = coMetadataFile.getPath.split("/").dropRight(1).mkString("/")
        val filePrefix = s"$coMetadataPath/original/g1/"
        val coMetadata = loadMetadataXml(coMetadataFile)
        val name = (coMetadata \ "ContentObject" \ "Title").text
        val fileId = UUID.fromString((coMetadata \ "ContentObject" \ "Ref").text)
        val storageRelativePath = files.find(_.getPath.startsWith(filePrefix)).map(_.getStorageRelativePath).getOrElse("")
        OcflFile(objectVersion.getVersionNum.getVersionNum, id, title, fileId, zref, s"${config.ocflRepoDir}/$storageRelativePath", name)
      }
    }
