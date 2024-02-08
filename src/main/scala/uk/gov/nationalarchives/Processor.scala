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
import uk.gov.nationalarchives.DisasterRecoveryItem._
import uk.gov.nationalarchives.Message._
import uk.gov.nationalarchives.Main.{Config, IdWithPath}
import uk.gov.nationalarchives.dp.client.Entities.fromType
import uk.gov.nationalarchives.dp.client.EntityClient.ContentObject
import uk.gov.nationalarchives.dp.client.{Entities, EntityClient}

import scala.xml.{Elem, PrettyPrinter}

class Processor(
    config: Config,
    sqsClient: DASQSClient[IO],
    service: OcflService,
    entityClient: EntityClient[IO, Fs2Streams[IO]]
) {
  private def deleteMessages(receiptHandles: List[String]): IO[List[DeleteMessageResponse]] =
    receiptHandles.map(handle => sqsClient.deleteMessage(config.sqsQueueUrl, handle)).sequence

  private def dedupeMessages(messages: List[Message]): List[Message] =
    messages.groupBy(_.show).map(_._2.last).toList

  private def createItem(entity: Entities.Entity, metadata: Seq[Elem]): List[MetadataItem] = {
    val newMetadata = <AllMetadata>
      {metadata}
    </AllMetadata>
    val xmlAsString = new PrettyPrinter(80, 2).format(newMetadata)
    val checksum = DigestUtils.sha256Hex(xmlAsString)
    List(MetadataItem(entity.ref, "tna-dr2-disaster-recovery-metadata.xml", checksum, newMetadata))
  }

  private def toDisasterRecoveryItem(message: Message): IO[List[DisasterRecoveryItem]] = message match {
    case InformationObjectMessage(ref) =>
      for {
        entity <- fromType[IO]("IO", ref, None, None, deleted = false)
        metadata <- entityClient.metadataForEntity(entity).map { metadata =>
          createItem(entity, metadata)
        }
      } yield metadata
    case ContentObjectMessage(ref) =>
      for {
        bitstreams <- entityClient.getBitstreamInfo(ref)
        entity <- entityClient.getEntity(ref, ContentObject)
        parent <- IO.fromOption(entity.parent)(new Exception("Cannot get IO reference from CO"))
      } yield bitstreams.toList.map(bitStreamInfo =>
        FileItem(parent, bitStreamInfo.name, bitStreamInfo.fixity.value, bitStreamInfo.url)
      )
  }

  private def download(disasterRecoveryItem: DisasterRecoveryItem) = disasterRecoveryItem match {
    case fi: FileItem =>
      for {
        writePath <- fi.path
        _ <- entityClient.streamBitstreamContent[Unit](Fs2Streams.apply)(
          fi.url,
          s => s.through(Files[IO].writeAll(writePath, Flags.Write)).compile.drain
        )
      } yield IdWithPath(fi.id, writePath.toNioPath)
    case mi: MetadataItem =>
      val prettyPrinter = new PrettyPrinter(80, 2)
      val formattedMetadata = prettyPrinter.format(mi.metadata)
      for {
        writePath <- mi.path
        _ <- Stream
          .emit(formattedMetadata)
          .through(Files[IO].writeUtf8(writePath))
          .compile
          .drain
      } yield IdWithPath(mi.id, writePath.toNioPath)
  }

  def process(messageResponses: List[MessageResponse[Option[Message]]]): IO[Unit] = {
    val messages = dedupeMessages(messageResponses.flatMap(_.message))
    for {
      logger <- Slf4jLogger.create[IO]
      _ <- logger.info(s"Processing ${messageResponses.length} messages")
      _ <- logger.info(s"Size after de-duplication ${messages.length}")
      _ <- logger.info(messages.map(_.show).mkString(","))
      disasterRecoveryItems <- messages.map(toDisasterRecoveryItem).sequence
      changedItemsPaths <- service.filterChangedItems(disasterRecoveryItems.flatten).map(download).sequence
      missingItemsPaths <- service.filterMissingItems(disasterRecoveryItems.flatten).map(download).sequence
      _ <- service.writeObjects(missingItemsPaths)
      _ <- service.updateObjects(changedItemsPaths)
      _ <- deleteMessages(messageResponses.map(_.receiptHandle))
    } yield ()
  }
}
object Processor {
  def apply(
      config: Config,
      sqsClient: DASQSClient[IO],
      service: OcflService,
      entityClient: EntityClient[IO, Fs2Streams[IO]]
  ): IO[Processor] = IO(
    new Processor(config, sqsClient, service, entityClient)
  )
}
