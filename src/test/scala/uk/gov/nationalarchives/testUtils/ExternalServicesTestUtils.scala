package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito, MockitoSugar}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}
import org.scalatest.{Assertion, EitherValues}
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.DisasterRecoveryObject.MetadataObject
import uk.gov.nationalarchives.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.Message.{ContentObjectMessage, InformationObjectMessage}
import uk.gov.nationalarchives.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives._
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient._

import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.xml.Elem

object ExternalServicesTestUtils extends MockitoSugar with EitherValues {
  private lazy val allRepresentationTypes: Map[String, RepresentationType] = Map(
    Access.toString -> Access,
    Preservation.toString -> Preservation
  )

  class ProcessorTestUtils(
      genVersion: Int = 1,
      genType: GenerationType = Original,
      parentRefExists: Boolean = true,
      urlsToRepresentations: Seq[String] = Seq("http://testurl/representations/Preservation/1"),
      missingObjects: List[DisasterRecoveryObject] = Nil,
      changedObjects: List[DisasterRecoveryObject] = Nil,
      throwErrorInMissingAndChangedObjects: Boolean = false
  ) {
    val config: Config = Config("", "", "queueUrl", "", "", Option(URI.create("https://example.com")))
    val ioId: UUID = UUID.randomUUID()
    val coId: UUID = UUID.randomUUID()
    val metadata: Elem = <Test><Metadata></Metadata></Test>
    lazy val coMessage: ContentObjectMessage = ContentObjectMessage(coId, s"co:$coId")
    lazy val ioMessage: InformationObjectMessage = InformationObjectMessage(ioId, s"io:$ioId")
    val sqsClient: DASQSClient[IO] = mock[DASQSClient[IO]]
    val entityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]

    val duplicatesIoMessageResponses: List[MessageResponse[Option[Message]]] =
      (1 to 3).toList.map(_ => MessageResponse[Option[Message]]("receiptHandle1", Option(ioMessage)))

    val duplicatesCoMessageResponses: List[MessageResponse[Option[Message]]] =
      (1 to 3).toList.map(_ => MessageResponse[Option[Message]]("receiptHandle2", Option(coMessage)))
    private val potentialParentRef = if (parentRefExists) Some(ioId) else None

    private val getBitstreamsCoIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    private val entityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])
    private val repTypeCaptor: ArgumentCaptor[RepresentationType] = ArgumentCaptor.forClass(classOf[RepresentationType])
    private val repIndexCaptor: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    private val queueUrlCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    private val idWithSourceAndDestPathsCaptor: ArgumentCaptor[List[IdWithSourceAndDestPaths]] =
      ArgumentCaptor.forClass(classOf[List[IdWithSourceAndDestPaths]])
    private val droLookupCaptor: ArgumentCaptor[List[DisasterRecoveryObject]] =
      ArgumentCaptor.forClass(classOf[List[DisasterRecoveryObject]])
    private val receiptHandleCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    private def mockOcflService(
        missingObjects: List[DisasterRecoveryObject] = Nil,
        changedObjects: List[DisasterRecoveryObject] = Nil,
        throwErrorInMissingAndChangedObjects: Boolean = false
    ): OcflService = {
      val ocflService = mock[OcflService]
      Mockito.reset(ocflService)
      if (throwErrorInMissingAndChangedObjects)
        when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
          .thenThrow(new Exception("Unexpected Error"))
      else
        when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
          .thenReturn(IO.pure(MissingAndChangedObjects(missingObjects, changedObjects)))
      when(ocflService.createObjects(any[List[IdWithSourceAndDestPaths]])).thenReturn(IO(Nil))

      ocflService
    }

    when(entityClient.getBitstreamInfo(coId))
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
              potentialParentRef
            )
          )
        )
      )
    potentialParentRef.foreach { parentRef =>
      when(entityClient.getUrlsToIoRepresentations(parentRef, None))
        .thenReturn(IO.pure(urlsToRepresentations))

      urlsToRepresentations.foreach { url =>
        val urlSplit = url.split("/").reverse
        val repType = allRepresentationTypes(urlSplit(1))
        val repIndex = urlSplit.head.toInt
        when(entityClient.getContentObjectsFromRepresentation(parentRef, repType, repIndex))
          .thenReturn(fromType[IO]("CO", coId, None, None, deleted = false, parent = Some(parentRef)).map(Seq(_)))
      }
    }

    when(entityClient.metadataForEntity(any[Entity])).thenReturn(IO.pure(Seq(metadata)))
    when(sqsClient.deleteMessage(queueUrlCaptor.capture, receiptHandleCaptor.capture))
      .thenReturn(IO.pure(DeleteMessageResponse.builder.build))

    val ocflService: OcflService = mockOcflService(missingObjects, changedObjects, throwErrorInMissingAndChangedObjects)

    val processor = new Processor(config, sqsClient, ocflService, entityClient)

    def verifyCallsAndArguments(
        numOfGetBitstreamInfoCalls: Int = 0,
        numOfGetUrlsToIoRepresentationsCalls: Int = 0,
        numOfGetContentObjectsFromRepresentationCalls: Int = 0,
        repTypes: List[RepresentationType] = List(Preservation),
        repIndexes: List[Int] = List(1),
        idsOfEntityToGetMetadataFrom: List[UUID] = List(ioId),
        entityTypesToGetMetadataFrom: List[EntityType] = List(InformationObject),
        createdIdSourceAndDestinationPathAndId: List[List[IdWithSourceAndDestPaths]] = Nil,
        drosToLookup: List[List[String]] = List(List(s"$ioId/IO_Metadata.xml")),
        receiptHandles: List[String] = List("receiptHandle", "receiptHandle", "receiptHandle")
    ): Assertion = {

      verify(entityClient, times(numOfGetBitstreamInfoCalls)).getBitstreamInfo(getBitstreamsCoIdCaptor.capture)
      verify(entityClient, times(numOfGetUrlsToIoRepresentationsCalls))
        .getUrlsToIoRepresentations(ArgumentMatchers.eq(ioId), ArgumentMatchers.eq(None))
      verify(entityClient, times(numOfGetContentObjectsFromRepresentationCalls)).getContentObjectsFromRepresentation(
        ArgumentMatchers.eq(ioId),
        repTypeCaptor.capture(),
        repIndexCaptor.capture()
      )
      verify(entityClient, times(idsOfEntityToGetMetadataFrom.length)).metadataForEntity(entityCaptor.capture)

      verify(ocflService, times(drosToLookup.length)).getMissingAndChangedObjects(
        droLookupCaptor.capture()
      )
      verify(ocflService, times(createdIdSourceAndDestinationPathAndId.length))
        .createObjects(idWithSourceAndDestPathsCaptor.capture())
      verify(sqsClient, times(receiptHandles.length))
        .deleteMessage(ArgumentMatchers.eq("queueUrl"), ArgumentMatchers.startsWith("receiptHandle"))

      getBitstreamsCoIdCaptor.getAllValues.asScala.toList should equal(
        (1 to numOfGetBitstreamInfoCalls).toList.map(_ => coId)
      )

      entityCaptor.getAllValues.asScala.toList
        .lazyZip(idsOfEntityToGetMetadataFrom)
        .lazyZip(entityTypesToGetMetadataFrom)
        .foreach { case (capturedEntity, idOfEntity, entityTypeToGetMetadataFrom) =>
          capturedEntity.ref should equal(idOfEntity)
          capturedEntity.entityType.map(_ should equal(entityTypeToGetMetadataFrom))
        }

      repTypeCaptor.getAllValues.asScala.toList should equal(repTypes)
      repIndexCaptor.getAllValues.asScala.toList should equal(repIndexes)

      val expectedIdsWithSourceAndDestPath = createdIdSourceAndDestinationPathAndId.flatten
      idWithSourceAndDestPathsCaptor.getAllValues.asScala.toList.flatten.zipWithIndex.foreach {
        case (capturedIdWithSourceAndDestPath, index) =>
          val expectedIdWithSourceAndDestPath = expectedIdsWithSourceAndDestPath(index)
          capturedIdWithSourceAndDestPath.id should equal(expectedIdWithSourceAndDestPath.id)
          capturedIdWithSourceAndDestPath.sourceNioFilePath.toString
            .endsWith(expectedIdWithSourceAndDestPath.sourceNioFilePath.toString) should equal(true)
          capturedIdWithSourceAndDestPath.destinationPath should equal(expectedIdWithSourceAndDestPath.destinationPath)
      }
      val capturedLookupDestinationPaths = droLookupCaptor.getAllValues.asScala.toList.map(_.map(_.destinationFilePath))
      capturedLookupDestinationPaths should equal(drosToLookup)
    }
  }
}
