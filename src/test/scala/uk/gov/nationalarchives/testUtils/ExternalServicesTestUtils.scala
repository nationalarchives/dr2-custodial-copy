package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.circe.Decoder
import io.ocfl.api.model.DigestAlgorithm
import io.ocfl.api.{OcflConfig, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.OcflStorageBuilder
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.{Assertion, EitherValues}
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.DisasterRecoveryObject.MetadataObject
import uk.gov.nationalarchives.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.Message.{ContentObjectMessage, InformationObjectMessage}
import uk.gov.nationalarchives.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives.OcflService.*
import uk.gov.nationalarchives.*
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.*

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.compat.java8.FunctionConverters.asJavaConsumer
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, NodeBuffer}

object ExternalServicesTestUtils extends MockitoSugar with EitherValues {
  private val ioType = InformationObject.entityTypeShort
  private val coType = ContentObject.entityTypeShort
  private lazy val allRepresentationTypes: Map[String, RepresentationType] = Map(
    Access.toString -> Access,
    Preservation.toString -> Preservation
  )

  private def mockPreservicaClient(
      ioId: UUID = UUID.fromString("049974f1-d3f0-4f51-8288-2a40051a663c"),
      coId: UUID = UUID.fromString("3393cd51-3c54-41a0-a9d4-5234a0ae47bf"),
      coId2: UUID = UUID.fromString("ed384a5c-689b-4f56-a47b-3690259a9998"),
      coId3: UUID = UUID.fromString("07747488-7976-4481-8fa4-b515c842d9a0"),
      metadataElems: Seq[Elem] = Nil,
      bitstreamInfo1: Seq[BitStreamInfo] = Nil,
      bitstreamInfo2: Seq[BitStreamInfo] = Nil,
      addAccessRepUrl: Boolean = false
  ): EntityClient[IO, Fs2Streams[IO]] = {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val entityType = Some(ContentObject)
    val contentObjectResponse =
      Entity(entityType, coId, None, None, deleted = false, entityType.map(_.entityPath), parent = Option(ioId))
    val urlToRepresentations = Seq(
      s"http://localhost/api/entity/information-objects/$ioId/representations/Preservation/1"
    ) ++ (if addAccessRepUrl then Seq(s"http://localhost/api/entity/information-objects/$ioId/representations/Access/1")
          else Nil)
    when(preservicaClient.metadataForEntity(any[Entity]))
      .thenReturn(IO(EntityMetadata(<Entity/>, <Identifier/>, metadataElems)))
    when(preservicaClient.getUrlsToIoRepresentations(ArgumentMatchers.eq(ioId), any[Option[RepresentationType]]))
      .thenReturn(IO(urlToRepresentations))
    when(
      preservicaClient.getContentObjectsFromRepresentation(
        ArgumentMatchers.eq(ioId),
        ArgumentMatchers.eq(Preservation),
        any[Int]
      )
    ).thenReturn(IO(Seq(contentObjectResponse, contentObjectResponse.copy(ref = coId2))))

    when(
      preservicaClient.getContentObjectsFromRepresentation(
        ArgumentMatchers.eq(ioId),
        ArgumentMatchers.eq(Access),
        any[Int]
      )
    ).thenReturn(IO(Seq(contentObjectResponse.copy(ref = coId3))))

    Seq((coId, bitstreamInfo1), (coId2, bitstreamInfo2), (coId3, bitstreamInfo1)).foreach { case (id, bitstreamInfo) =>
      when(preservicaClient.getBitstreamInfo(ArgumentMatchers.eq(id))).thenReturn(IO(bitstreamInfo))
    }

    Seq(coId, coId2).foreach { id =>
      when(preservicaClient.getEntity(id, ContentObject))
        .thenReturn(IO(contentObjectResponse.copy(ref = id)))
    }
    (bitstreamInfo1 ++ bitstreamInfo2).foreach { bitstreamInfo =>
      when(
        preservicaClient
          .streamBitstreamContent[Unit](any[Fs2Streams[IO]])(any[String], any())
      ).thenAnswer((invocation: InvocationOnMock) => {
        val stream = invocation.getArgument[Fs2Streams[IO]#BinaryStream => IO[Unit]](2)
        if Option(stream).isDefined then
          stream(Stream.emits(s"File content for ${bitstreamInfo.name}".getBytes)).unsafeRunSync()

        IO.unit
      })
    }

    preservicaClient
  }

  private def createExistingMetadataEntryInRepo(
      id: UUID,
      entityType: String,
      repo: OcflRepository,
      elem: NodeBuffer,
      destinationPath: String
  ) = {
    val existingMetadata = <AllMetadata>
      {elem}
    </AllMetadata>
    val xmlAsString = existingMetadata.toString()
    addFileToRepo(id, repo, xmlAsString, metadataFile(id, entityType), destinationPath)
  }

  private def addFileToRepo(
      id: UUID,
      repo: OcflRepository,
      bodyAsString: String,
      sourceFilePath: String,
      destinationPath: String
  ) = {
    val path = Files.createTempDirectory(id.toString)
    Files.createDirectories(Paths.get(path.toString, id.toString))
    val fullSourceFilePath = Paths.get(path.toString, sourceFilePath)
    Files.write(fullSourceFilePath, bodyAsString.getBytes)
    new OcflService(repo)
      .createObjects(List(IdWithSourceAndDestPaths(id, fullSourceFilePath, destinationPath)))
      .unsafeRunSync()
  }

  private def createTestRepo(repoDir: Path = Files.createTempDirectory("repo")) = {
    val workDir = Files.createTempDirectory("work")
    new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(asJavaConsumer[OcflStorageBuilder](s => s.fileSystem(repoDir)))
      .ocflConfig(asJavaConsumer[OcflConfig](config => config.setDefaultDigestAlgorithm(DigestAlgorithm.sha256)))
      .prettyPrintJson()
      .workDir(workDir)
      .build()
  }

  private def mockSqs(messages: List[Message]): DASQSClient[IO] = {
    val sqsClient = mock[DASQSClient[IO]]
    val responses = IO {
      messages.zipWithIndex.map { case (message, idx) =>
        MessageResponse[Option[Message]](s"handle$idx", Option(message))
      }
    }
    when(sqsClient.receiveMessages[Option[Message]](any[String], any[Int])(using any[Decoder[Option[Message]]]))
      .thenReturn(responses)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder().build))
    sqsClient
  }

  private def metadataFile(id: UUID, entityType: String, potentialPath: Option[String] = None) =
    s"$id/${potentialPath.map(_ + "/").getOrElse("")}${entityType}_Metadata.xml"

  class MainTestUtils(
      typesOfSqsMessages: List[EntityType] = List(InformationObject),
      objectVersion: Int = 1,
      typesOfMetadataFilesInRepo: List[EntityType] = Nil,
      fileContentToWriteToEachFileInRepo: List[String] = Nil,
      metadataElemsPreservicaResponse: Seq[Elem] = Seq(<Test></Test>),
      bitstreamInfo1Responses: Seq[BitStreamInfo] = Seq(
        BitStreamInfo("name1", 1, "", Fixity("SHA256", ""), 1, Original, None, Some(UUID.randomUUID()))
      ),
      bitstreamInfo2Responses: Seq[BitStreamInfo] = Nil,
      addAccessRepUrl: Boolean = false
  ) {

    val config: Config = Config("", "", "", "", "", None, "")

    val bitstreamInfoResponsesWithSameName: Seq[BitStreamInfo] = bitstreamInfo1Responses.flatMap {
      bitstreamInfo1Response =>
        bitstreamInfo2Responses.filter { bitstreamInfo2Response =>
          bitstreamInfo1Response.name == bitstreamInfo2Response.name
        }
    }

    val coId1: UUID = UUID.randomUUID()
    val coId2: UUID = UUID.randomUUID()
    val coId3: UUID = if bitstreamInfoResponsesWithSameName.nonEmpty then coId1 else UUID.randomUUID()
    val ioId: UUID = bitstreamInfo1Responses.headOption.flatMap(_.parentRef).getOrElse(UUID.randomUUID())
    lazy val repoDir: Path = Files.createTempDirectory("repo")
    lazy val repo: OcflRepository = createTestRepo(repoDir)

    private val coIds: Seq[UUID] = List(coId1, coId2, coId3)
    private val sqsMessages: List[Message] =
      typesOfSqsMessages.zipWithIndex.flatMap { case (entityType, index) =>
        entityType match { // create duplicates in order to test deduplication
          case InformationObject => (1 to 2).map(_ => InformationObjectMessage(ioId, s"${ioType.toLowerCase}:$ioId"))
          case ContentObject =>
            val coId = coIds(index)
            (1 to 2).map(_ => ContentObjectMessage(coId, s"${coType.toLowerCase}:$coId"))
          case unexpectedEntityType => throw new Exception(s"Unexpected EntityType $unexpectedEntityType!")
        }
      }
    val sqsClient: DASQSClient[IO] = mockSqs(sqsMessages)

    lazy val expectedIoMetadataFileDestinationPath: String = metadataFile(ioId, ioType)
    lazy val expectedCoMetadataFileDestinationPath: String = metadataFile(ioId, coType, Some(s"Preservation_1/$coId1"))
    lazy val expectedCoFileDestinationPath: String = s"$ioId/Preservation_1/$coId1/original/g1/name1"

    val preservicaClient: EntityClient[IO, Fs2Streams[IO]] = mockPreservicaClient(
      ioId,
      coId1,
      coId2,
      coId3,
      metadataElems = metadataElemsPreservicaResponse,
      bitstreamInfo1 = bitstreamInfo1Responses,
      bitstreamInfo2 = bitstreamInfo2Responses,
      addAccessRepUrl
    )

    private val metadataInRepo = <Test></Test><Entity/><Identifier/>

    typesOfMetadataFilesInRepo.foreach {
      case InformationObject =>
        createExistingMetadataEntryInRepo(
          ioId,
          ioType,
          repo,
          metadataInRepo,
          expectedIoMetadataFileDestinationPath
        )
      case ContentObject =>
        createExistingMetadataEntryInRepo(
          ioId,
          coType,
          repo,
          metadataInRepo,
          expectedCoMetadataFileDestinationPath
        )
      case unexpectedEntityType => throw new Exception(s"Unexpected EntityType $unexpectedEntityType!")
    }

    fileContentToWriteToEachFileInRepo.zip(LazyList.from(1)).foreach { case (data, index) =>
      addFileToRepo(ioId, repo, data, s"$ioId/name$index", s"$expectedCoFileDestinationPath".dropRight(1) + index)
    }

    val ocflService = new OcflService(repo)
    val processor = new Processor(config, sqsClient, ocflService, preservicaClient)

    def latestObjectVersion(repo: OcflRepository, id: UUID): Long =
      repo.getObject(id.toHeadVersion).getObjectVersionId.getVersionNum.getVersionNum

    private val potentialObjectVersion: Try[Long] = Try(latestObjectVersion(repo, ioId))

    potentialObjectVersion match {
      case Success(actualVersion)                              => actualVersion should equal(objectVersion)
      case Failure(_: io.ocfl.api.exception.NotFoundException) => objectVersion should equal(0)
      case _                                                   => throw new Exception("Unexpected result")
    }
  }

  class ProcessorTestUtils(
      genVersion: Int = 1,
      genType: GenerationType = Original,
      parentRefExists: Boolean = true,
      urlsToRepresentations: Seq[String] = Seq("http://testurl/representations/Preservation/1"),
      missingObjects: List[DisasterRecoveryObject] = Nil,
      changedObjects: List[DisasterRecoveryObject] = Nil,
      throwErrorInMissingAndChangedObjects: Boolean = false
  ) {
    val config: Config = Config("", "", "queueUrl", "", "", Option(URI.create("https://example.com")), "")
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
    private val potentialParentRef = if parentRefExists then Some(ioId) else None

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
      if throwErrorInMissingAndChangedObjects then
        when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
          .thenThrow(new RuntimeException("Unexpected Error"))
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

    when(entityClient.metadataForEntity(any[Entity]))
      .thenReturn(IO.pure(EntityMetadata(<Entity/>, <Identifier/>, metadata)))
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
