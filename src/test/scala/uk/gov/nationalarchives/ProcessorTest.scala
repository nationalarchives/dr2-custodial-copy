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
import uk.gov.nationalarchives.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.Message.{ContentObjectMessage, InformationObjectMessage}
import uk.gov.nationalarchives.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{Original, Preservation}

import java.net.URI
import java.util.UUID

class ProcessorTest extends AnyFlatSpec with MockitoSugar {
  val config: Config = Config("", "", "queueUrl", "", "", Option(URI.create("https://example.com")))

  private def mockOcflService(
      missingObjects: List[DisasterRecoveryObject] = Nil,
      changedObjects: List[DisasterRecoveryObject] = Nil,
      throwErrorInMissingAndChangedObjects: Boolean = false
  ): OcflService = {
    val ocflService = mock[OcflService]
    if (throwErrorInMissingAndChangedObjects)
      when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
        .thenThrow(new Exception("Unexpected Error"))
    else
      when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
        .thenReturn(IO.pure(MissingAndChangedObjects(missingObjects, changedObjects)))
    when(ocflService.updateObjects(any[List[IdWithSourceAndDestPaths]])).thenReturn(IO(Nil))
    when(ocflService.createObjects(any[List[IdWithSourceAndDestPaths]])).thenReturn(IO(Nil))

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
    val parentRef = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val coIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val genType = Original
    val genVersion = 1

    when(entityClient.getBitstreamInfo(coIdCaptor.capture))
      .thenReturn(
        IO(
          Seq(
            BitStreamInfo(
              "name",
              1,
              "url",
              Fixity("sha256", "checksum"),
              genVersion,
              genType,
              Some("CoTitle"),
              Some(parentRef)
            )
          )
        )
      )
    when(entityClient.getUrlsToIoRepresentations(parentRef, None))
      .thenReturn(IO(Seq("http://testurl/representations/Preservation/1")))
    when(entityClient.getContentObjectsFromRepresentation(parentRef, Preservation, genVersion))
      .thenReturn(fromType[IO]("CO", id, None, None, false, parent = Some(parentRef)).map(Seq(_)))
    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(<Test><Metadata></Metadata></Test>)))

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

    val ocflService = mockOcflService(
      Nil,
      List(MetadataObject(id, Some("Preservation_1"), "changed", "checksum", metadata, "destinationPath"))
    )

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    val updateCaptor: ArgumentCaptor[List[IdWithSourceAndDestPaths]] =
      ArgumentCaptor.forClass(classOf[List[IdWithSourceAndDestPaths]])

    verify(ocflService, times(1)).updateObjects(updateCaptor.capture)
    verify(ocflService, times(1)).createObjects(ArgumentMatchers.eq(Nil))

    updateCaptor.getValue.length should equal(1)
    updateCaptor.getValue.head.sourceNioFilePath.toString.endsWith(s"$id/changed") should equal(true)
  }

  "process" should "create but not update if the object is missing" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val metadata = <Test><Metadata></Metadata></Test>

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(metadata)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService(
      List(MetadataObject(id, Some("Preservation_1"), "missing", "checksum", metadata, "destinationPath")),
      Nil
    )

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    processor.process(responses).unsafeRunSync()

    val createCaptor: ArgumentCaptor[List[IdWithSourceAndDestPaths]] =
      ArgumentCaptor.forClass(classOf[List[IdWithSourceAndDestPaths]])

    verify(ocflService, times(1)).updateObjects(ArgumentMatchers.eq(Nil))
    verify(ocflService, times(1)).createObjects(createCaptor.capture)

    createCaptor.getValue.length should equal(1)
    createCaptor.getValue.head.sourceNioFilePath.toString.endsWith(s"$id/missing") should equal(true)
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
      List(MetadataObject(missingId, Some("Preservation_1"), "missing", "checksum", metadata, "destinationPath")),
      List(MetadataObject(changedId, Some("Preservation_1"), "changed", "checksum", metadata, "destinationPath"))
    )

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] = List(
      MessageResponse[Option[Message]]("receiptHandle1", Option(InformationObjectMessage(missingId, s"io:$missingId"))),
      MessageResponse[Option[Message]]("receiptHandle2", Option(InformationObjectMessage(missingId, s"io:$changedId")))
    )
    processor.process(responses).unsafeRunSync()

    val updateCaptor: ArgumentCaptor[List[IdWithSourceAndDestPaths]] =
      ArgumentCaptor.forClass(classOf[List[IdWithSourceAndDestPaths]])
    val createCaptor: ArgumentCaptor[List[IdWithSourceAndDestPaths]] =
      ArgumentCaptor.forClass(classOf[List[IdWithSourceAndDestPaths]])

    verify(ocflService, times(1)).updateObjects(updateCaptor.capture)
    verify(ocflService, times(1)).createObjects(createCaptor.capture)

    val update = updateCaptor.getValue
    val create = createCaptor.getValue

    update.length should equal(1)
    update.head.sourceNioFilePath.toString.endsWith(s"$changedId/changed") should equal(true)

    create.length should equal(1)
    create.head.sourceNioFilePath.toString.endsWith(s"$missingId/missing") should be(true)
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

  "process" should "throw an Exception if an Unexpected Exception was returned from the OCFL service" in {
    val id = UUID.randomUUID()
    val sqsClient = mock[DASQSClient[IO]]
    val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO(Seq(<Test><Metadata></Metadata></Test>)))
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))

    val ocflService = mockOcflService(throwErrorInMissingAndChangedObjects = true)

    val processor = new Processor(config, sqsClient, ocflService, entityClient)
    val responses: List[MessageResponse[Option[Message]]] =
      MessageResponse[Option[Message]]("receiptHandle", Option(InformationObjectMessage(id, s"io:$id"))) :: Nil
    val res = processor.process(responses).attempt.unsafeRunSync()

    res.left.foreach(_.getMessage should equal("Unexpected Error"))

    verify(sqsClient, times(0)).deleteMessage(any[String], any[String])
  }
}
