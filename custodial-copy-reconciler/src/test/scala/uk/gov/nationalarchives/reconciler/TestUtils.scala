package uk.gov.nationalarchives.reconciler

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import io.circe.Encoder
import io.ocfl.api.model.OcflObjectVersionFile
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DAEventBridgeClient
import uk.gov.nationalarchives.dp.client.Client.BitStreamInfo
import uk.gov.nationalarchives.dp.client.{DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.utils.Detail
import uk.gov.nationalarchives.utils.TestUtils.notImplemented

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

object TestUtils:

  def testOcflService(objectVersionFiles: List[OcflObjectVersionFile]): OcflService[IO] = new OcflService[IO]() {
    override def getAllObjectFiles(ioId: UUID): IO[List[OcflObjectVersionFile]] = IO.pure(objectVersionFiles)
  }

  def testEntityClient(idToBitstreams: Map[UUID, List[BitStreamInfo]]): EntityClient[IO, Fs2Streams[IO]] = new TestEntityClient {
    override def getBitstreamInfo(contentRef: UUID): IO[Seq[BitStreamInfo]] = IO.pure(idToBitstreams.getOrElse(contentRef, Nil))
  }

  def testEntityClient(entities: List[Entities.EntityRef], bitstreams: List[BitStreamInfo]): EntityClient[IO, Fs2Streams[IO]] = new TestEntityClient {
    override def streamAllEntityRefs(repTypeFilter: Option[EntityClient.RepresentationType]): fs2.Stream[IO, Entities.EntityRef] =
      fs2.Stream.emits[IO, Entities.EntityRef](entities)

    override def getBitstreamInfo(contentRef: UUID): IO[Seq[BitStreamInfo]] = IO.pure(bitstreams)
  }

  def eventBridgeClient(ref: Ref[IO, List[Detail]]): DAEventBridgeClient[IO] = new DAEventBridgeClient[IO] {
    override def publishEventToEventBridge[T, U](sourceId: String, detailType: U, detail: T)(using enc: Encoder[T]): IO[PutEventsResponse] =
      ref.update(existing => detail.asInstanceOf[Detail] :: existing).map(_ => PutEventsResponse.builder.build)
  }

  def runTestReconciler(entities: List[Entities.EntityRef], bitstreams: List[BitStreamInfo])(using configuration: Configuration): List[Detail] = (for {
    detailRef <- Ref.of[IO, List[Detail]](Nil)
    ocflService = OcflService[IO](configuration.config)
    _ <- Main.runReconciler(testEntityClient(entities, bitstreams), ocflService, eventBridgeClient(detailRef))
    eventBridgeDetails <- detailRef.get
  } yield eventBridgeDetails).unsafeRunSync()

  class TestEntityClient extends EntityClient[IO, Fs2Streams[IO]]:
    override def streamAllEntityRefs(repTypeFilter: Option[EntityClient.RepresentationType]): fs2.Stream[IO, Entities.EntityRef] =
      fs2.Stream.raiseError(new Exception("Not implemented"))

    override def getBitstreamInfo(contentRef: UUID): IO[Seq[BitStreamInfo]] = notImplemented

    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

    override def metadataForEntity(entity: Entities.Entity): IO[EntityClient.EntityMetadata] = notImplemented

    override def getEntity(entityRef: UUID, entityType: EntityClient.EntityType): IO[Entities.Entity] = notImplemented

    override def getEntityIdentifiers(entity: Entities.Entity): IO[Seq[Entities.IdentifierResponse]] = notImplemented

    override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = notImplemented

    override def getContentObjectsFromRepresentation(
        ioEntityRef: UUID,
        representationType: EntityClient.RepresentationType,
        repTypeIndex: Int
    ): IO[Seq[Entities.Entity]] = notImplemented

    override def addEntity(addEntityRequest: EntityClient.AddEntityRequest): IO[UUID] = notImplemented

    override def updateEntity(updateEntityRequest: EntityClient.UpdateEntityRequest): IO[String] = notImplemented

    override def updateEntityIdentifiers(entity: Entities.Entity, identifiers: Seq[Entities.IdentifierResponse]): IO[Seq[Entities.IdentifierResponse]] =
      notImplemented

    override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] =
      notImplemented

    override def entitiesUpdatedSince(dateTime: ZonedDateTime, startEntry: Int, maxEntries: Int): IO[Seq[Entities.Entity]] = notImplemented

    override def entityEventActions(entity: Entities.Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = notImplemented

    override def entitiesPerIdentifier(identifiers: Seq[EntityClient.Identifier]): IO[Map[EntityClient.Identifier, Seq[Entities.Entity]]] = notImplemented

    override def addIdentifierForEntity(entityRef: UUID, entityType: EntityClient.EntityType, identifier: EntityClient.Identifier): IO[String] = notImplemented

    override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = notImplemented
