package uk.gov.nationalarchives.custodialcopy.testUtils

import cats.effect.IO
import sttp.capabilities.Streams
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.Client.BitStreamInfo
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction
import uk.gov.nationalarchives.dp.client.Entities.{Entity, FullAsset, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.EntityClient

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

class TestPreservicaClient(fullAsset: FullAsset) extends EntityClient[IO, Fs2Streams[IO]] {

  override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

  override def getFullAsset(ioRef: UUID): IO[FullAsset] =
    IO.pure(fullAsset)

  override def streamBitstreamContent[T](stream: Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] =
    IO.stub

  override def metadataForEntity(entity: Entity): IO[EntityMetadata] = IO.stub
  override def bitstreamForAsset(entityRef: UUID): IO[Seq[BitStreamInfo]] = IO.stub
  override def getBitstreamInfo(contentRef: UUID): IO[Seq[BitStreamInfo]] = IO.stub
  override def getEntity(entityRef: UUID, entityType: EntityType): IO[Entity] = IO.stub
  override def getEntityIdentifiers(entity: Entity): IO[Seq[IdentifierResponse]] = IO.stub
  override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[RepresentationType]): IO[Seq[String]] = IO.stub
  override def getContentObjectsFromRepresentation(
      ioEntityRef: UUID,
      representationType: RepresentationType,
      repTypeIndex: Int
  ): IO[Seq[Entity]] = IO.stub
  override def addEntity(addEntityRequest: AddEntityRequest): IO[UUID] = IO.stub
  override def updateEntity(updateEntityRequest: UpdateEntityRequest): IO[String] = IO.stub
  override def updateEntityIdentifiers(entity: Entity, identifiers: Seq[IdentifierResponse]): IO[Seq[IdentifierResponse]] = IO.stub
  override def getAllAssetIds(maxConcurrency: Int): fs2.Stream[IO, UUID] = fs2.Stream.empty
  override def entitiesUpdatedSince(
      sinceDateTime: ZonedDateTime,
      startEntry: Int,
      maxEntries: Int,
      potentialEndDate: Option[ZonedDateTime]
  ): IO[EntitiesUpdated] = IO.stub
  override def entityEventActions(entity: Entity, startEntry: Int, maxEntries: Int): IO[Seq[EventAction]] = IO.stub
  override def entitiesPerIdentifier(identifiers: Seq[Identifier]): IO[Map[Identifier, Seq[Entity]]] = IO.stub
  override def addIdentifierForEntity(entityRef: UUID, entityType: EntityType, identifier: Identifier): IO[String] = IO.stub
  override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = IO.stub
}
