package uk.gov.nationalarchives.disasterrecovery

import cats.effect.IO
import cats.implicits.*
import fs2.Stream
import fs2.io.file.{Files, Flags}
import org.apache.commons.codec.digest.DigestUtils
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.disasterrecovery.DisasterRecoveryObject.*
import uk.gov.nationalarchives.disasterrecovery.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.disasterrecovery.Message.*
import uk.gov.nationalarchives.dp.client.Entities.fromType
import uk.gov.nationalarchives.dp.client.{EntityClient, ValidateXmlAgainstXsd}
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd.PreservicaSchema.XipXsdSchemaV7

import java.util.UUID
import scala.xml.Elem

class Processor(
    config: Config,
    sqsClient: DASQSClient[IO],
    ocflService: OcflService,
    entityClient: EntityClient[IO, Fs2Streams[IO]],
    xmlValidator: ValidateXmlAgainstXsd[IO]
) {
  private val newlineAndIndent = "\n          "
  private def deleteMessages(receiptHandles: List[String]): IO[List[DeleteMessageResponse]] =
    receiptHandles.map(handle => sqsClient.deleteMessage(config.sqsQueueUrl, handle)).sequence

  private def dedupeMessages(messages: List[Message]): List[Message] = messages.distinctBy(_.messageText)

  private def createMetadataObject(
      ioRef: UUID,
      metadata: EntityMetadata,
      fileName: String,
      path: String,
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
          ++ metadata.eventActions.flatMap(eventAction => Seq(newlineAndIndent, eventAction))

      allMetadataAsXml =
        <XIP xmlns="http://preservica.com/XIP/v7.0">
          {consolidatedMetadata}
        </XIP>

      allMetadataAsXmlString = allMetadataAsXml.toString()

      _ <- xmlValidator.xmlStringIsValid(allMetadataAsXmlString)
    } yield List(
      MetadataObject(ioRef, repType, fileName, DigestUtils.sha256Hex(allMetadataAsXmlString), allMetadataAsXml, path)
    )

  private lazy val allRepresentationTypes: Map[String, RepresentationType] = Map(
    Access.toString -> Access,
    Preservation.toString -> Preservation
  )

  private def getRepresentationTypeOfCo(ioRef: UUID, urlOfRepresentation: String, coRef: UUID) = {
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
    } yield contentObjectsFromRep.collect {
      case contentObjectFromRep if contentObjectFromRep.ref == coRef => s"${representationType}_$index"
    }
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

  private def toDisasterRecoveryObject(message: Message): IO[List[DisasterRecoveryObject]] = message match {
    case InformationObjectMessage(ref, _) =>
      for {
        entity <- fromType[IO](InformationObject.entityTypeShort, ref, None, None, deleted = false)
        metadataFileName = createMetadataFileName(InformationObject.entityTypeShort)
        metadataObject <- entityClient.metadataForEntity(entity).flatMap { metadata =>
          val destinationFilePath = createDestinationFilePath(entity.ref, fileName = metadataFileName)
          createMetadataObject(entity.ref, metadata, metadataFileName, destinationFilePath)
        }
      } yield metadataObject
    case ContentObjectMessage(ref, _) =>
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
            metadataFragments,
            metadataFileName,
            destinationFilePath,
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
        FileObject(parentRef, bitStreamInfo.name, bitStreamInfo.fixity.value, bitStreamInfo.url, destinationFilePath)
      } ++ metadata
  }

  private def download(disasterRecoveryObject: DisasterRecoveryObject) = disasterRecoveryObject match {
    case fo: FileObject =>
      for {
        writePath <- fo.sourceFilePath
        _ <- entityClient.streamBitstreamContent[Unit](Fs2Streams.apply)(
          fo.url,
          s => s.through(Files[IO].writeAll(writePath, Flags.Write)).compile.drain
        )
      } yield IdWithSourceAndDestPaths(fo.id, writePath.toNioPath, fo.destinationFilePath)
    case mo: MetadataObject =>
      val metadataXmlAsString = mo.metadata.toString
      for {
        writePath <- mo.sourceFilePath
        _ <- Stream
          .emit(metadataXmlAsString)
          .through(Files[IO].writeUtf8(writePath))
          .compile
          .drain
      } yield IdWithSourceAndDestPaths(mo.id, writePath.toNioPath, mo.destinationFilePath)
  }

  def process(messageResponses: List[MessageResponse[Option[Message]]]): IO[Unit] =
    for {
      logger <- Slf4jLogger.create[IO]
      _ <- logger.info(s"Processing ${messageResponses.length} messages")
      messages = dedupeMessages(messageResponses.flatMap(_.message))
      _ <- logger.info(s"Size after de-duplication ${messages.length}")
      _ <- logger.info(messages.map(_.messageText).mkString(","))
      disasterRecoveryObjects <- messages.map(toDisasterRecoveryObject).sequence
      flatDisasterRecoveryObjects = disasterRecoveryObjects.flatten

      missingAndChangedObjects <- ocflService.getMissingAndChangedObjects(flatDisasterRecoveryObjects)

      missingObjectsPaths <- missingAndChangedObjects.missingObjects.map(download).sequence
      changedObjectsPaths <- missingAndChangedObjects.changedObjects.map(download).sequence

      _ <- ocflService.createObjects(missingObjectsPaths)
      _ <- logger.info(s"${missingObjectsPaths.length} objects created")
      _ <- ocflService.createObjects(changedObjectsPaths)
      _ <- logger.info(s"${changedObjectsPaths.length} objects updated")
      _ <- deleteMessages(messageResponses.map(_.receiptHandle))
    } yield ()
}
object Processor {
  def apply(
      config: Config,
      sqsClient: DASQSClient[IO],
      ocflService: OcflService,
      entityClient: EntityClient[IO, Fs2Streams[IO]]
  ): IO[Processor] = IO(
    new Processor(config, sqsClient, ocflService, entityClient, ValidateXmlAgainstXsd[IO](XipXsdSchemaV7))
  )
}
