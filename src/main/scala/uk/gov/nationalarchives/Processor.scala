package uk.gov.nationalarchives

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import fs2.io.file.{Files, Flags}
import org.apache.commons.codec.digest.DigestUtils
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.DisasterRecoveryObject._
import uk.gov.nationalarchives.Message._
import uk.gov.nationalarchives.Main.{Config, IdWithPath}
import uk.gov.nationalarchives.dp.client.Entities.fromType
import uk.gov.nationalarchives.dp.client.EntityClient.ContentObject
import uk.gov.nationalarchives.dp.client.{Entities, EntityClient}

import scala.xml.Elem

class Processor(
    config: Config,
    sqsClient: DASQSClient[IO],
    ocflService: OcflService,
    entityClient: EntityClient[IO, Fs2Streams[IO]]
) {
  private def deleteMessages(receiptHandles: List[String]): IO[List[DeleteMessageResponse]] =
    receiptHandles.map(handle => sqsClient.deleteMessage(config.sqsQueueUrl, handle)).sequence

  private def dedupeMessages(messages: List[Message]): List[Message] = messages.distinctBy(_.messageText)

  private def createMetadataObject(entity: Entities.Entity, metadata: Seq[Elem]): List[MetadataObject] = {
    val newMetadata = <AllMetadata>
      {metadata}
    </AllMetadata>
    val xmlAsString = newMetadata.toString()
    val checksum = DigestUtils.sha256Hex(xmlAsString)
    List(MetadataObject(entity.ref, "tna-dr2-disaster-recovery-metadata.xml", checksum, newMetadata))
  }

  private def toDisasterRecoveryObject(message: Message): IO[List[DisasterRecoveryObject]] = message match {
    case InformationObjectMessage(ref, _) =>
      for {
        entity <- fromType[IO](EntityClient.InformationObject.entityTypeShort, ref, None, None, deleted = false)
        metadata <- entityClient.metadataForEntity(entity).map { metadata =>
          createMetadataObject(entity, metadata)
        }
      } yield metadata
    case ContentObjectMessage(ref, _) =>
      for {
        bitstreams <- entityClient.getBitstreamInfo(ref)
        entity <- entityClient.getEntity(ref, ContentObject)
        parent <- IO.fromOption(entity.parent)(new Exception("Cannot get IO reference from CO"))
      } yield bitstreams.toList.map(bitStreamInfo =>
        FileObject(parent, bitStreamInfo.name, bitStreamInfo.fixity.value, bitStreamInfo.url)
      )
  }

  private def download(disasterRecoveryObject: DisasterRecoveryObject) = disasterRecoveryObject match {
    case fo: FileObject =>
      for {
        writePath <- fo.path
        _ <- entityClient.streamBitstreamContent[Unit](Fs2Streams.apply)(
          fo.url,
          s => s.through(Files[IO].writeAll(writePath, Flags.Write)).compile.drain
        )
      } yield IdWithPath(fo.id, writePath.toNioPath)
    case mo: MetadataObject =>
      val metadataXmlAsString = mo.metadata.toString
      for {
        writePath <- mo.path
        _ <- Stream
          .emit(metadataXmlAsString)
          .through(Files[IO].writeUtf8(writePath))
          .compile
          .drain
      } yield IdWithPath(mo.id, writePath.toNioPath)
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
      _ <- ocflService.updateObjects(changedObjectsPaths)
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
    new Processor(config, sqsClient, ocflService, entityClient)
  )
}
