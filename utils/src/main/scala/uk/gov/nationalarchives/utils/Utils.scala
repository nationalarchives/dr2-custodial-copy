package uk.gov.nationalarchives.utils

import cats.effect.{Async, Sync}
import cats.syntax.all.*
import doobie.util.{Get, Put}
import io.circe.Decoder
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId, OcflObjectVersionFile}
import io.ocfl.api.{OcflConfig, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.{OcflStorage, OcflStorageBuilder}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.DASQSClient.MessageResponse

import java.net.URI
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.xml.XML
object Utils:

  given Get[UUID] = Get[String].map(UUID.fromString)

  given Put[UUID] = Put[String].contramap(_.toString)

  given Get[Instant] = Get[Long].map(Instant.ofEpochMilli)

  given Put[Instant] = Put[Long].contramap(_.toEpochMilli)

  final case class OcflFile(
      versionNum: Long,
      id: UUID,
      name: Option[String],
      fileId: UUID,
      zref: Option[String],
      path: Option[String],
      fileName: Option[String],
      ingestDateTime: Option[Instant],
      sourceId: Option[String],
      citation: Option[String],
      consignmentRef: Option[String],
      code: Option[String]
  )

  extension (uuid: UUID) def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)

  def generateOcflObjects(id: UUID, repository: OcflRepository, repoDir: String): List[OcflFile] = {
    val objectVersion = repository.getObject(ObjectVersionId.head(id.toString))
    val files = objectVersion.getFiles.asScala.toList

    def loadMetadataXml(file: OcflObjectVersionFile) = XML.loadString(Files.readString(Paths.get(repoDir, file.getStorageRelativePath)))

    val ioMetadata = files.find(_.getPath.contains(s"IO_Metadata.xml")).map(loadMetadataXml)

    def getIdentifier(identifierName: String) = ioMetadata
      .flatMap { metadata =>
        (metadata \ "Identifier").toList
          .find(i => (i \ "Type").text == identifierName)
          .map(i => (i \ "Value").text)
      }

    val zref = getIdentifier("BornDigitalRef")
    val sourceId = getIdentifier("SourceID")
    val citation = getIdentifier("NeutralCitation")
    val consignmentRef = getIdentifier("ConsignmentReference")
    val code = getIdentifier("Code")

    val title = ioMetadata
      .flatMap { metadata =>
        {
          val titleText = (metadata \ "InformationObject" \ "Title").text
          if (titleText.isEmpty) None else Option(titleText)
        }
      }

    val ingestDateTime = ioMetadata.flatMap { metadata =>
      val dateTimeText = (metadata \ "Metadata" \ "Content" \ "Source" \ "IngestDateTime").text
      if dateTimeText.isBlank then None else Option(Instant.parse(dateTimeText))
    }

    files.filter(_.getPath.contains(s"CO_Metadata.xml")).map { coMetadataFile =>
      val coMetadataPath = coMetadataFile.getPath.split("/").dropRight(1).mkString("/")
      val filePrefix = s"$coMetadataPath/original/g1/"
      val coMetadata = loadMetadataXml(coMetadataFile)
      val name = (coMetadata \ "ContentObject" \ "Title").text
      val nameOpt = if (name.isBlank) None else Option(name)
      val fileId = UUID.fromString((coMetadata \ "ContentObject" \ "Ref").text)
      val storageRelativePathOpt = files.find(_.getPath.startsWith(filePrefix)).map(_.getStorageRelativePath)
      OcflFile(
        objectVersion.getVersionNum.getVersionNum,
        id,
        title,
        fileId,
        zref,
        storageRelativePathOpt.map(path => s"$repoDir/$path"),
        nameOpt,
        ingestDateTime,
        sourceId,
        citation,
        consignmentRef,
        code
      )
    }
  }

  def createOcflRepository(ocflRepoDir: String, ocflWorkDir: String): OcflRepository = {
    val repoDir = Paths.get(ocflRepoDir)
    val workDir = Paths.get(ocflWorkDir)
    val storage: OcflStorage = OcflStorageBuilder.builder().fileSystem(repoDir).build
    val ocflConfig: OcflConfig = new OcflConfig()
    ocflConfig.setDefaultDigestAlgorithm(DigestAlgorithm.fromOcflName("sha256"))
    new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(storage)
      .ocflConfig(ocflConfig)
      .prettyPrintJson()
      .workDir(workDir)
      .build()
  }

  def aggregateMessages[F[_]: Sync, T: Decoder](sqs: DASQSClient[F], sqsQueueUrl: String): F[List[MessageResponse[T]]] = {
    def collectMessages(messages: List[MessageResponse[T]]): F[List[MessageResponse[T]]] = {
      sqs
        .receiveMessages[T](sqsQueueUrl)
        .flatMap { newMessages =>
          val allMessages = newMessages ++ messages
          if newMessages.isEmpty || allMessages.size >= 50 then Sync[F].pure(allMessages) else collectMessages(allMessages)
        }
        .handleErrorWith { err =>
          logError(err) >> Sync[F].pure[List[MessageResponse[T]]](messages)
        }
    }

    collectMessages(Nil)
  }

  def sqsClient[F[_]: Async](proxyUrl: Option[URI]): DASQSClient[F] =
    proxyUrl
      .map(proxy => DASQSClient[F](proxy))
      .getOrElse(DASQSClient[F]())

  def logError[F[_]: Sync](err: Throwable): F[Unit] = for {
    logger <- Slf4jLogger.create[F]
    _ <- logger.error(err)("There has been an error")
  } yield ()

trait OcflServiceConfig:
  val ocflRepoDir: String
  val ocflWorkDir: String

enum DetailType:
  case DR2Message, DR2DevMessage

case class Detail(slackMessage: String)
