package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import io.circe.Encoder
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{equal, should}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatestplus.mockito.MockitoSugar.mock
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DAEventBridgeClient
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef, NoEntityRef, StructuralObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.{Configuration, OcflService}
import uk.gov.nationalarchives.utils.DetailType.DR2DevMessage
import uk.gov.nationalarchives.utils.TestUtils.*
import uk.gov.nationalarchives.utils.{Detail, DetailType}

import java.util.UUID
import scala.jdk.CollectionConverters.*

class MainSpec extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {

  val databaseUtils = new DatabaseUtils("test-database")
  import databaseUtils.*

  override def beforeAll(): Unit = {
    createActuallyInPsTable()
    createExpectedInPsTable()
  }

  private lazy val httpClient: SdkAsyncHttpClient = NettyNioAsyncHttpClient.builder().build()

  given config: Configuration = new Configuration:
    override def config: Config = Config("", databaseName, 5, "ocflRepoDir", "ocflWorkDir")

  Chunk(StructuralObjectRef(coRef, Some(ioRef)), NoEntityRef)

  "runReconciler" should "not process non-InformationObjectRefs nor non-ContentObjectRefs" in {
    val id = UUID.randomUUID()
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val eventBridgeClient = mock[DAEventBridgeClient[IO]]
    val sourceIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val detailTypeCaptor: ArgumentCaptor[DetailType] = ArgumentCaptor.forClass(classOf[DetailType])
    val detailCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    when(preservicaClient.streamAllEntityRefs(any[Option[EntityClient.RepresentationType]]))
      .thenReturn(fs2.Stream.emits(List(StructuralObjectRef(coRef, Some(ioRef)), NoEntityRef)))
    when(
      eventBridgeClient.publishEventToEventBridge(
        any[String],
        any[DetailType],
        any[String]
      )(using any[Encoder[String]])
    ).thenReturn(IO(mock[PutEventsResponse]))

    val ocflService = mock[OcflService]

    Main.runReconciler(preservicaClient, ocflService, eventBridgeClient).compile.drain.unsafeRunSync()

    verify(preservicaClient, times(1)).streamAllEntityRefs(any[Option[EntityClient.RepresentationType]])
    verify(eventBridgeClient, times(0)).publishEventToEventBridge(any[String], any[DetailType], any[String])(using any[Encoder[String]])
    verify(preservicaClient, times(0)).getBitstreamInfo(any[UUID])
  }

  "runReconciler" should "filter out non-CO files and non-original CO files returned from OCFL" in {
    val id = UUID.randomUUID
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val eventBridgeClient = mock[DAEventBridgeClient[IO]]
    val sourceIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val detailTypeCaptor: ArgumentCaptor[DetailType] = ArgumentCaptor.forClass(classOf[DetailType])
    val detailCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val (repoDir, workDir) = initialiseRepo(ioRef, addContentFilesToRepo = false)
    val coRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    given config: Configuration = new Configuration:
      override def config: Config = Config("", "test-database", 5, repoDir, workDir)

    when(preservicaClient.streamAllEntityRefs(any[Option[EntityClient.RepresentationType]]))
      .thenReturn(fs2.Stream.emits(List(InformationObjectRef(ioRef, id), ContentObjectRef(coRef, ioRef))))
    when(preservicaClient.getBitstreamInfo(coRefCaptor.capture())).thenReturn(IO.pure(Nil))
    when(
      eventBridgeClient.publishEventToEventBridge(
        sourceIdCaptor.capture(),
        detailTypeCaptor.capture(),
        detailCaptor.capture()
      )(using any[Encoder[String]])
    ).thenReturn(IO(mock[PutEventsResponse]))

    val ocflService = OcflService(config.config).unsafeRunSync()

    Main.runReconciler(preservicaClient, ocflService, eventBridgeClient).compile.drain.unsafeRunSync()

    verify(preservicaClient, times(1)).streamAllEntityRefs(any[Option[EntityClient.RepresentationType]])
    verify(eventBridgeClient, times(0)).publishEventToEventBridge(any[String], any[DetailType], any[String])(using any[Encoder[String]])
    verify(preservicaClient, times(1)).getBitstreamInfo(any[UUID])
    coRefCaptor.getValue should equal(coRef)
  }

  "runReconciler" should "send a message to EventBridge if a CO in Preservica could not be found in CC or vice versa" in {
    val id = UUID.randomUUID
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val eventBridgeClient = mock[DAEventBridgeClient[IO]]
    val sourceIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val detailTypeCaptor: ArgumentCaptor[DetailType] = ArgumentCaptor.forClass(classOf[DetailType])
    val detailCaptor: ArgumentCaptor[Detail] = ArgumentCaptor.forClass(classOf[Detail])
    val (repoDir, workDir) = initialiseRepo(ioRef)
    val coRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val bitStreamInfo = BitStreamInfo(
      s"$coRef.testExt",
      1,
      "https://example.com",
      List(),
      1,
      Original,
      None,
      Some(ioRef)
    )

    given config: Configuration = new Configuration:
      override def config: Config = Config("", "test-database", 5, repoDir, workDir)

    when(preservicaClient.streamAllEntityRefs(any[Option[EntityClient.RepresentationType]]))
      .thenReturn(fs2.Stream.emits(List(InformationObjectRef(ioRef, id), ContentObjectRef(coRef, ioRef))))
    when(preservicaClient.getBitstreamInfo(coRefCaptor.capture())).thenReturn(IO.pure(Seq(bitStreamInfo)))
    when(eventBridgeClient.publishEventToEventBridge[Detail, DetailType](any[String], any[DetailType], any[Detail])(using any[Encoder[Detail]]))
      .thenReturn(IO(mock[PutEventsResponse]))

    val ocflService = OcflService(config.config).unsafeRunSync()

    Main.runReconciler(preservicaClient, ocflService, eventBridgeClient).compile.drain.unsafeRunSync()

    verify(preservicaClient, times(1)).streamAllEntityRefs(any[Option[EntityClient.RepresentationType]])
    verify(eventBridgeClient, times(2))
      .publishEventToEventBridge[Detail, DetailType](sourceIdCaptor.capture(), detailTypeCaptor.capture(), detailCaptor.capture())(using any[Encoder[Detail]])
    sourceIdCaptor.getAllValues.asScala.toList should equal(List("uk.gov.nationalarchives.reconciler.Main$", "uk.gov.nationalarchives.reconciler.Main$"))
    detailTypeCaptor.getAllValues.asScala.toList should equal(List(DR2DevMessage, DR2DevMessage))
    detailCaptor.getAllValues.asScala.toList should equal(
      List(
        Detail(s"CO $coRef (parent: $ioRef) is in Preservica, but its checksum could not be found in CC"),
        Detail(
          s"CO $coRef (parent: $ioRef) is in CC, but its checksum could not be found in Preservica\n" +
            s"CO $coRef (parent: $ioRef) is in CC, but its checksum could not be found in Preservica"
        )
      )
    )
    verify(preservicaClient, times(1)).getBitstreamInfo(any[UUID])
    coRefCaptor.getValue should equal(coRef)
  }

  "runReconciler" should "not send a message to EventBridge if a CO in Preservica could be found in CC or vice versa" in {
    val id = UUID.randomUUID
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val eventBridgeClient = mock[DAEventBridgeClient[IO]]
    val sourceIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val detailTypeCaptor: ArgumentCaptor[DetailType] = ArgumentCaptor.forClass(classOf[DetailType])
    val detailCaptor: ArgumentCaptor[Detail] = ArgumentCaptor.forClass(classOf[Detail])
    val (repoDir, workDir) = initialiseRepo(ioRef)
    val coRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val bitStreamInfo = BitStreamInfo(
      s"$coRef.testExt",
      1,
      "https://example.com",
      List(Fixity("SHA256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")),
      1,
      Original,
      None,
      Some(ioRef)
    )

    given config: Configuration = new Configuration:
      override def config: Config = Config("", "test-database", 5, repoDir, workDir)

    when(preservicaClient.streamAllEntityRefs(any[Option[EntityClient.RepresentationType]]))
      .thenReturn(fs2.Stream.emits(List(InformationObjectRef(ioRef, id), ContentObjectRef(coRef, ioRef))))
    when(preservicaClient.getBitstreamInfo(coRefCaptor.capture())).thenReturn(IO.pure(Seq(bitStreamInfo)))
    when(eventBridgeClient.publishEventToEventBridge[Detail, DetailType](any[String], any[DetailType], any[Detail])(using any[Encoder[Detail]]))
      .thenReturn(IO(mock[PutEventsResponse]))

    val ocflService = OcflService(config.config).unsafeRunSync()

    Main.runReconciler(preservicaClient, ocflService, eventBridgeClient).compile.drain.unsafeRunSync()

    verify(preservicaClient, times(1)).streamAllEntityRefs(any[Option[EntityClient.RepresentationType]])
    verify(eventBridgeClient, times(0)).publishEventToEventBridge[Detail, DetailType](any[String], any[DetailType], any[Detail])(using any[Encoder[Detail]])
    verify(preservicaClient, times(1)).getBitstreamInfo(any[UUID])
    coRefCaptor.getValue should equal(coRef)
  }
}
