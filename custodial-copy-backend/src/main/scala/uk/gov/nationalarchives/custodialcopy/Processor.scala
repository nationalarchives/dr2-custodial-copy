package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.implicits.*
import fs2.Stream
import fs2.io.file.{Files, Flags}

import java.nio.file.{Files as JFiles, Path as JPath}
import io.circe.Encoder
import org.apache.commons.codec.digest.DigestUtils
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityResponse, DeleteMessageResponse}
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.{DASNSClient, DASQSClient}
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.custodialcopy.CustodialCopyObject.*
import uk.gov.nationalarchives.custodialcopy.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.custodialcopy.Message.*
import uk.gov.nationalarchives.custodialcopy.Processor.{ObjectStatus, Result}
import uk.gov.nationalarchives.custodialcopy.Processor.Result.*
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectStatus.{Created, Updated}
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectType.{Bitstream, Metadata}
import uk.gov.nationalarchives.dp.client.{EntityClient, ValidateXmlAgainstXsd}
import uk.gov.nationalarchives.dp.client.Client.BitStreamInfo
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd.PreservicaSchema.XipXsdSchemaV7

import java.util.UUID
import scala.annotation.tailrec
import scala.xml.{Elem, Node}
import scala.concurrent.duration.*

class Processor(
    config: Config,
    sqsClient: DASQSClient[IO],
    ocflService: OcflService,
    entityClient: EntityClient[IO, Fs2Streams[IO]],
    xmlValidator: ValidateXmlAgainstXsd[IO],
    snsClient: DASNSClient[IO]
) {
  private val newlineAndIndent = "\n          "

  private val changeVisibilityTimeout: (String, Duration) => IO[ChangeMessageVisibilityResponse] = sqsClient.changeVisibilityTimeout(config.sqsQueueUrl)

  def sendHeartbeat(receiptHandle: String): IO[Unit] =
    IO.sleep(config.queueTimeout) >> changeVisibilityTimeout(receiptHandle, config.queueTimeout) >> sendHeartbeat(receiptHandle)

  def deleteMessage(receiptHandle: String): IO[DeleteMessageResponse] =
    sqsClient.deleteMessage(config.sqsQueueUrl, receiptHandle)

  private def createMetadataObject(
      ioRef: UUID,
      entityType: EntityType,
      metadata: EntityMetadata,
      fileName: String,
      path: String,
      tableItemIdentifier: String,
      repType: Option[String] = None
  ): IO[List[MetadataObject]] =
    for {
      entitySpecificMetadata <-
        metadata match {
          case ioMetadata: IoMetadata =>
            IO(
              Seq(ioMetadata.entityNode)
                ++ ioMetadata.representations.flatMap(representation => Seq(newlineAndIndent, representation))
            )
          case coMetadata: CoMetadata =>
            IO(
              Seq(coMetadata.entityNode)
                ++ coMetadata.generationNodes.flatMap(generationNode => Seq(newlineAndIndent, generationNode))
                ++ coMetadata.bitstreamNodes.flatMap(bitstreamNode => Seq(newlineAndIndent, bitstreamNode))
            )
          case _ => IO.raiseError(new Exception(s"${metadata.getClass} is not a supported metadata type"))
        }

      consolidatedMetadata =
        entitySpecificMetadata
          ++ metadata.identifiers.flatMap(identifier => Seq(newlineAndIndent, identifier))
          ++ metadata.links.flatMap(link => Seq(newlineAndIndent, link))
          ++ metadata.metadataNodes.flatMap(metadataNode => Seq(newlineAndIndent, metadataNode))
          ++ metadata.eventActions
            .filterNot(_.attribute("commandType").map(_.toString).contains("command_download"))
            .flatMap(eventAction => Seq(newlineAndIndent, eventAction))

      allMetadataAsXml =
        <XIP xmlns="http://preservica.com/XIP/v7.7">
          {consolidatedMetadata}
        </XIP>

      allMetadataAsXmlString = allMetadataAsXml.toString()

      _ <- xmlValidator.xmlStringIsValid(allMetadataAsXmlString)
    } yield List(
      MetadataObject(
        ioRef,
        entityType,
        repType,
        fileName,
        List(Checksum("SHA256", DigestUtils.sha256Hex(allMetadataAsXmlString))),
        allMetadataAsXml,
        path,
        tableItemIdentifier
      )
    )

  private lazy val allRepresentationTypes: Map[String, RepresentationType] = Map(
    Access.toString -> Access,
    Preservation.toString -> Preservation
  )

  private def getCosPerRepresentationType(ioRef: UUID, urlOfRepresentation: String): IO[Seq[(Entity, String)]] = {
    val splitUrlReversed = urlOfRepresentation.split("/").reverse
    val index = splitUrlReversed.head.toInt
    val representationTypeAsString = splitUrlReversed(1)

    val representationType = allRepresentationTypes(representationTypeAsString)

    for {
      contentObjectsFromRep <- entityClient.getContentObjectsFromRepresentation(
        ioRef,
        representationType,
        index
      )
    } yield contentObjectsFromRep.map(contentObjectFromRep => contentObjectFromRep -> s"${representationType}_$index")
  }

  private def getRepresentationTypeOfCo(ioRef: UUID, urlOfRepresentation: String, coRef: UUID): IO[Seq[String]] = {
    val cosPerRepresentationTypeIO = getCosPerRepresentationType(ioRef: UUID, urlOfRepresentation: String)
    cosPerRepresentationTypeIO.map(_.collect { case (co, representationType) if co.ref == coRef => representationType }.toSeq)
  }

  private def createDestinationFilePath(
      ioRef: UUID,
      potentialRef: Option[UUID] = None,
      potentialRepTypeGroup: Option[String] = None,
      potentialGenType: Option[GenerationType] = None,
      potentialGenVersion: Option[Int] = None,
      fileName: String
  ): String =
    List(
      Some(ioRef),
      potentialRepTypeGroup,
      potentialRef,
      potentialGenType.map(_.toString.toLowerCase),
      potentialGenVersion.map(version => s"g$version"),
      Some(fileName)
    ).flatten.mkString("/")

  private def createMetadataFileName(entityTypeShort: String) = s"${entityTypeShort}_Metadata.xml"

  private def getCoFileAndMetadataObjects(
      entity: Entity,
      parentRef: UUID,
      coRepTypes: Seq[String],
      bitstreamInfoPerCo: Seq[BitStreamInfo]
  ): IO[List[CustodialCopyObject]] =
    for {
      bitstreamIdentifiers <- IO.pure(bitstreamInfoPerCo.map(_.name).map(removeFileExtension).toSet)
      _ <- IO.raiseWhen(bitstreamIdentifiers.size != 1)(new Exception(s"Expected 1 bitstream identifiers, found ${bitstreamIdentifiers.size}"))
      _ <- IO.raiseWhen(coRepTypes.length > 1) {
        new Exception(s"${entity.ref} belongs to more than 1 representation type: ${coRepTypes.mkString(", ")}")
      }

      representationTypeGroup = coRepTypes.headOption
      metadataFileName = createMetadataFileName(ContentObject.entityTypeShort)
      metadata <- entityClient.metadataForEntity(entity).flatMap { metadataFragments =>
        val destinationFilePath = createDestinationFilePath(
          parentRef,
          Some(entity.ref),
          representationTypeGroup,
          fileName = metadataFileName
        )
        createMetadataObject(
          parentRef,
          entity.entityType.get,
          metadataFragments,
          metadataFileName,
          destinationFilePath,
          bitstreamIdentifiers.head,
          representationTypeGroup
        )
      }
    } yield bitstreamInfoPerCo.toList.map { bitStreamInfo =>
      val destinationFilePath = createDestinationFilePath(
        parentRef,
        Some(entity.ref),
        representationTypeGroup,
        Some(bitStreamInfo.generationType),
        Some(bitStreamInfo.generationVersion),
        bitStreamInfo.name
      )

      FileObject(
        parentRef,
        bitStreamInfo.name,
        bitStreamInfo.fixities.map(eachFixity => Checksum(eachFixity.algorithm, eachFixity.value)),
        bitStreamInfo.url,
        destinationFilePath,
        removeFileExtension(bitStreamInfo.name)
      )
    } ++ metadata

  private def toCustodialCopyObject(receivedSnsMessage: ReceivedSnsMessage): IO[List[CustodialCopyObject]] = receivedSnsMessage match {
    case IoReceivedSnsMessage(ref) =>
      for {
        entity <- fromType[IO](InformationObject.entityTypeShort, ref, None, None, deleted = false)
        metadataFileName = createMetadataFileName(InformationObject.entityTypeShort)
        metadataObject <- entityClient.metadataForEntity(entity).flatMap { metadata =>
          val destinationFilePath = createDestinationFilePath(entity.ref, fileName = metadataFileName)
          createMetadataObject(
            entity.ref,
            entity.entityType.get,
            metadata,
            metadataFileName,
            destinationFilePath,
            getSourceIdFromIdentifierNodes(metadata.identifiers)
          )
        }
      } yield metadataObject
    case CoReceivedSnsMessage(ref) =>
      for {
        bitstreamInfoPerCo <- entityClient.getBitstreamInfo(ref)
        entity <- fromType[IO](
          ContentObject.entityTypeShort,
          ref,
          None,
          None,
          deleted = false,
          parent = bitstreamInfoPerCo.headOption.flatMap(_.parentRef)
        )
        parentRef <- IO.fromOption(entity.parent)(new Exception("Cannot get IO reference from CO"))
        urlsOfRepresentations <- entityClient.getUrlsToIoRepresentations(parentRef, None)
        coRepTypes <- urlsOfRepresentations.map(getRepresentationTypeOfCo(parentRef, _, entity.ref)).flatSequence
        coFileAndMetadataObjects <- getCoFileAndMetadataObjects(entity, parentRef, coRepTypes, bitstreamInfoPerCo)
      } yield coFileAndMetadataObjects

    case _ => IO.pure(Nil)
  }

  private def download(custodialCopyObject: CustodialCopyObject, ioId: UUID) = custodialCopyObject match {
    case fo: FileObject =>
      if fo.url.nonEmpty then
        ocflService.fileInRepository(fo, ioId).flatMap { isFileInRepository =>
          if isFileInRepository then IO.pure(IdWithSourceAndDestPaths(fo.id, None, fo.destinationFilePath))
          else
            for
              writePath <- fo.sourceFilePath(config.downloadDir)
              _ <- entityClient.streamBitstreamContent[Unit](Fs2Streams.apply)(
                fo.url,
                s => s.through(Files[IO].writeAll(writePath, Flags.Write)).compile.drain
              )
            yield IdWithSourceAndDestPaths(fo.id, Option(writePath.toNioPath), fo.destinationFilePath)
        }
      else IO.pure(IdWithSourceAndDestPaths(fo.id, None, fo.destinationFilePath))

    case mo: MetadataObject =>
      val metadataXmlAsString = mo.metadata.toString
      for {
        writePath <- mo.sourceFilePath(config.downloadDir)
        _ <- Stream
          .emit(metadataXmlAsString)
          .through(Files[IO].writeUtf8(writePath))
          .compile
          .drain
      } yield IdWithSourceAndDestPaths(mo.id, Option(writePath.toNioPath), mo.destinationFilePath)
  }

  private def removeFileExtension(bitstreamName: String) =
    if (bitstreamName.contains(".")) bitstreamName.split('.').dropRight(1).mkString(".") else bitstreamName

  private def getSourceIdFromIdentifierNodes(identifiers: Seq[Node]) = identifiers
    .collectFirst {
      case identifier if (identifier \ "Type").text == "SourceID" => (identifier \ "Value").text
    }
    .getOrElse("")

  private def generateSnsMessage(
      obj: CustodialCopyObject,
      receivedSnsMessage: ReceivedSnsMessage,
      status: ObjectStatus
  ): Option[SendSnsMessage] = {
    val objectType = obj match {
      case _: FileObject     => Bitstream
      case _: MetadataObject => Metadata
    }

    val potentialEntityType = receivedSnsMessage match
      case _: IoReceivedSnsMessage => Option(InformationObject)
      case _: CoReceivedSnsMessage => Option(ContentObject)
      case _: SoReceivedSnsMessage => Option(StructuralObject)
      case _                       => None

    potentialEntityType.map(entityType => SendSnsMessage(entityType, obj.id, objectType, status, obj.tableItemIdentifier))
  }

  given Encoder[SendSnsMessage] = (message: SendSnsMessage) => {
    Encoder
      .forProduct5("entityType", "ioRef", "objectType", "status", "tableItemIdentifier")(_ =>
        (message.entityType.toString, message.ioRef.toString, message.objectType.toString, message.status.toString, message.tableItemIdentifier)
      )
      .apply(message)
  }

  private def processNonDeletedMessages(messageResponse: MessageResponse[ReceivedSnsMessage]): IO[List[SendSnsMessage]] =
    val downloadFile = download(_, UUID.fromString(messageResponse.messageGroupId.get))
    for {
      logger <- Slf4jLogger.create[IO]
      custodialCopyObjects <- toCustodialCopyObject(messageResponse.message)

      _ <- custodialCopyObjects.traverse(obj => logger.info(Map("sourceId" -> obj.tableItemIdentifier))(s"Processing object ${obj.id}"))

      missingAndChangedObjects <- ocflService.getMissingAndChangedObjects(custodialCopyObjects)
      (missingIoObjects, missingCoObjects) = missingAndChangedObjects.missingObjects.partition {
        case mo: MetadataObject => mo.entityType == InformationObject
        case _                  => false
      }

      missingIosAndCos <- missingIoObjects.flatTraverse { missingIoObject =>
        for {
          urlsOfRepresentations <- entityClient.getUrlsToIoRepresentations(missingIoObject.id, None)
          cosAndRepType <- urlsOfRepresentations.flatTraverse { getCosPerRepresentationType(missingIoObject.id, _) }
          coAndRepTypesGroupedByCoRef = cosAndRepType.groupBy { case (co, _) => co.ref }.toList

          allCoObjectsOfIo <- coAndRepTypesGroupedByCoRef.flatTraverse { case (coRef, coAndRepTypes) =>
            entityClient.getBitstreamInfo(coRef).flatMap { bitstreamInfoPerCo =>
              val (entity, _) = coAndRepTypes.head
              val repTypes = coAndRepTypes.map { case (_, repType) => repType }
              getCoFileAndMetadataObjects(entity, missingIoObject.id, repTypes, bitstreamInfoPerCo)
            }
          }
        } yield allCoObjectsOfIo :+ missingIoObject
      }

      allMissingObjects: List[CustodialCopyObject] = missingIosAndCos ++ missingCoObjects
      missingObjectsPaths <- allMissingObjects.traverse(downloadFile)
      changedObjectsPaths <- missingAndChangedObjects.changedObjects.traverse(downloadFile)

      _ <- ocflService.createObjects(missingObjectsPaths)
      _ <- logger.info(s"${missingObjectsPaths.length} objects created")
      _ <- ocflService.createObjects(changedObjectsPaths)
      _ <- logger.info(s"${changedObjectsPaths.length} objects updated")

      _ <- (missingObjectsPaths ++ changedObjectsPaths).flatMap(_.sourceNioFilePath).parTraverse(deleteObjectPath)

      createdObjsSnsMessages = missingAndChangedObjects.missingObjects.flatMap(generateSnsMessage(_, messageResponse.message, Created))
      updatedObjsSnsMessages = missingAndChangedObjects.changedObjects.flatMap(generateSnsMessage(_, messageResponse.message, Updated))

    } yield createdObjsSnsMessages ++ updatedObjsSnsMessages

  @tailrec
  private def deleteObjectPath(path: JPath): IO[Unit] = {
    if JPath.of(config.downloadDir) == path then IO.unit
    else
      JFiles.deleteIfExists(path)
      deleteObjectPath(path.getParent)
  }

  private def processDeletedEntities(messageResponse: MessageResponse[ReceivedSnsMessage]): IO[List[SendSnsMessage]] =
    val ref = messageResponse.message.ref
    for {
      filePaths <- ocflService.getAllFilePathsOnAnObject(ref)
      _ <- ocflService.deleteObjects(ref, filePaths)
    } yield Nil

  def process(messageResponse: MessageResponse[ReceivedSnsMessage]): IO[Result] = {
    for {
      logger <- Slf4jLogger.create[IO]
      snsMessages <-
        messageResponse.message match
          case DeletionReceivedSnsMessage(ref) => processDeletedEntities(messageResponse)
          case _                               => processNonDeletedMessages(messageResponse)
      _ <- IO.whenA(snsMessages.nonEmpty) {
        snsClient.publish[SendSnsMessage](config.topicArn)(snsMessages).void
      }
      _ <- logger.info(s"${snsMessages.length} 'created/updated objects' messages published to SNS")
    } yield Success(messageResponse.message.ref)
  }.handleError(err => Failure(err))

  def commitStagedChanges(id: UUID): IO[Unit] = ocflService.commitStagedChanges(id)
}
object Processor {
  def apply(
      config: Config,
      sqsClient: DASQSClient[IO],
      ocflService: OcflService,
      entityClient: EntityClient[IO, Fs2Streams[IO]],
      snsClient: DASNSClient[IO]
  ): IO[Processor] = IO(
    new Processor(config, sqsClient, ocflService, entityClient, ValidateXmlAgainstXsd[IO](XipXsdSchemaV7), snsClient)
  )

  enum ObjectStatus:
    case Created, Updated

  enum ObjectType:
    case Bitstream, Metadata, MetadataAndPotentialBitstreams

  enum Result:
    def isSuccess: Boolean = this match
      case _: Result.Success => true
      case _: Result.Failure => false

    def isError: Boolean = !isSuccess

    case Success(id: UUID)
    case Failure(ex: Throwable)
}
