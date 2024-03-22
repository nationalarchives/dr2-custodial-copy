package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.DisasterRecoveryObject.MetadataObject
import uk.gov.nationalarchives.Main.{Config, IdWithPath}
import uk.gov.nationalarchives.Message.{ContentObjectMessage, InformationObjectMessage}
import uk.gov.nationalarchives.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.ContentObject

import java.net.URI
import java.util.UUID

class ProcessorTest extends AnyFlatSpec with MockitoSugar {
  val config: Config = Config("", "", "queueUrl", "", "", Option(URI.create("https://example.com")))

  private def mockOcflService(
      missingObjects: List[DisasterRecoveryObject] = Nil,
      changedObjects: List[DisasterRecoveryObject] = Nil
  ): OcflService = {
    val ocflService = mock[OcflService]
    when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(IO.pure(MissingAndChangedObjects(missingObjects, changedObjects)))
    when(ocflService.updateObjects(any[List[IdWithPath]])).thenReturn(IO(Nil))
    when(ocflService.createObjects(any[List[IdWithPath]])).thenReturn(IO(Nil))

    ocflService
  }

  "process" should "download the metadata if an information object update is received" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val entityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])

    when(entityClient.metadataForEntity(entityCaptor.capture)).thenReturn(IO(Seq(<Test><Metadata></Metadata></Test>)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService()
    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    entityCaptor.getValue.ref should equal(id)
  }

  "process" should "retrieve the bitstream for a CO update message" in {
    val id = UUID.randomUUID()
    val parent = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val coIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    when(entityClient.getBitstreamInfo(coIdCaptor.capture))
      .thenReturn(IO(Seq(BitStreamInfo("name", 1, "url", Fixity("sha256", "checksum")))))
    when(entityClient.getEntity(id, ContentObject))
      .thenReturn(fromType[IO]("CO", id, None, None, false, parent = Option(parent)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService()

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(ContentObjectMessage(id, s"co:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    coIdCaptor.getValue should equal(id)
  }

  "process" should "create or update no objects if no objects are missing or changed" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(<Test><Metadata></Metadata></Test>)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService()

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    verify(ocflService, times(1)).updateObjects(ArgumentMatchers.eq(Nil))
    verify(ocflService, times(1)).createObjects(ArgumentMatchers.eq(Nil))
  }

  "process" should "update but not create if the object has changed" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val metadata = <Test><Metadata></Metadata></Test>

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(metadata)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService(Nil, List(MetadataObject(id, "changed", "checksum", metadata)))

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    val updateCaptor: ArgumentCaptor[List[IdWithPath]] = ArgumentCaptor.forClass(classOf[List[IdWithPath]])

    verify(ocflService, times(1)).updateObjects(updateCaptor.capture)
    verify(ocflService, times(1)).createObjects(ArgumentMatchers.eq(Nil))

    updateCaptor.getValue.length should equal(1)
    updateCaptor.getValue.head.path.toString.endsWith(s"$id/changed") should equal(true)
  }

  "process" should "create but not update if the object is missing" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val metadata = <Test><Metadata></Metadata></Test>

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(metadata)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService(List(MetadataObject(id, "missing", "checksum", metadata)), Nil)

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    val createCaptor: ArgumentCaptor[List[IdWithPath]] = ArgumentCaptor.forClass(classOf[List[IdWithPath]])

    verify(ocflService, times(1)).updateObjects(ArgumentMatchers.eq(Nil))
    verify(ocflService, times(1)).createObjects(createCaptor.capture)

    createCaptor.getValue.length should equal(1)
    createCaptor.getValue.head.path.toString.endsWith(s"$id/missing") should equal(true)
  }

  "process" should "update a changed file and add a missing file" in {
    val missingId = UUID.randomUUID()
    val changedId = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val metadata = <Test><Metadata></Metadata></Test>

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(metadata)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService(
      List(MetadataObject(missingId, "missing", "checksum", metadata)),
      List(MetadataObject(changedId, "changed", "checksum", metadata))
    )

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] = List(
      MessageResponse[Option[Message]]("receiptHandle1", Option(InformationObjectMessage(missingId, s"io:$missingId"))),
      MessageResponse[Option[Message]]("receiptHandle2", Option(InformationObjectMessage(missingId, s"io:$changedId")))
    )
    processor.process(responses).unsafeRunSync()

    val updateCaptor: ArgumentCaptor[List[IdWithPath]] = ArgumentCaptor.forClass(classOf[List[IdWithPath]])
    val createCaptor: ArgumentCaptor[List[IdWithPath]] = ArgumentCaptor.forClass(classOf[List[IdWithPath]])

    verify(ocflService, times(1)).updateObjects(updateCaptor.capture)
    verify(ocflService, times(1)).createObjects(createCaptor.capture)

    val update = updateCaptor.getValue
    val create = createCaptor.getValue

    update.length should equal(1)
    update.head.path.toString.endsWith(s"$changedId/changed") should equal(true)

    create.length should equal(1)
    create.head.path.toString.endsWith(s"$missingId/missing") should be(true)
  }

  "process" should "delete the messages if all messages are written successfully" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(<Test><Metadata></Metadata></Test>)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService()

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    verify(sqsClient, times(1)).deleteMessage(ArgumentMatchers.eq("queueUrl"), ArgumentMatchers.eq("receiptHandle"))
  }

  "process" should "not delete the messages if there is a failure" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]

    when(entityClient.metadataForEntity(any[Entity])).thenThrow(new RuntimeException("Error getting metadata"))

    val ocflService = mockOcflService()

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).attempt.unsafeRunSync()

    verify(sqsClient, times(0)).deleteMessage(any[String], any[String])
  }
}
