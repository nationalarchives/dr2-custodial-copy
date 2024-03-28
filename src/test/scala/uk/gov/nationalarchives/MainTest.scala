package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Decoder
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId}
import io.ocfl.api.{OcflConfig, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.OcflStorageBuilder
import org.mockito.ArgumentMatchers._
import fs2.Stream
import org.apache.commons.codec.digest.DigestUtils
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers._
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.Main._
import uk.gov.nationalarchives.Message._
import uk.gov.nationalarchives.OcflService.UuidUtils
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{Access, Derived, Original, Preservation, RepresentationType}

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.compat.java8.FunctionConverters.asJavaConsumer
import scala.xml.Elem

class MainTest extends AnyFlatSpec with MockitoSugar with EitherValues {
  private val ioType = "io"
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

  private val config = Config("", "", "", "", "", None)

  private def mockSqs(messages: List[Message]): DASQSClient[IO] = {
    val sqsClient = mock[DASQSClient[IO]]
    val responses = IO {
      messages.zipWithIndex.map { case (message, idx) =>
        MessageResponse[Option[Message]](s"handle$idx", Option(message))
      }
    }
    when(sqsClient.receiveMessages[Option[Message]](any[String], any[Int])(any[Decoder[Option[Message]]]))
      .thenReturn(responses)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder().build))
    sqsClient
  }

  private def mockPreservicaClient(
      ioId: UUID = UUID.fromString("049974f1-d3f0-4f51-8288-2a40051a663c"),
      coId: UUID = UUID.fromString("3393cd51-3c54-41a0-a9d4-5234a0ae47bf"),
      coId2: UUID = UUID.fromString("ed384a5c-689b-4f56-a47b-3690259a9998"),
      coId3: UUID = UUID.fromString("07747488-7976-4481-8fa4-b515c842d9a0"),
      elems: Seq[Elem] = Nil,
      bitstreamInfo1: Seq[BitStreamInfo] = Nil,
      bitstreamInfo2: Seq[BitStreamInfo] = Nil,
      addAccessRepUrl: Boolean = false
  ): EntityClient[IO, Fs2Streams[IO]] = {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val entityType = Some(EntityClient.ContentObject)
    val contentObjectResponse =
      Entity(entityType, coId, None, None, false, entityType.map(_.entityPath), parent = Option(ioId))
    val urlToRepresentations = Seq(
      s"http://localhost/api/entity/information-objects/$ioId/representations/Preservation/1"
    ) ++ (if (addAccessRepUrl) Seq(s"http://localhost/api/entity/information-objects/$ioId/representations/Access/1")
          else Nil)
    when(preservicaClient.metadataForEntity(any[Entity])).thenReturn(IO(elems))
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
      when(preservicaClient.getEntity(id, EntityClient.ContentObject))
        .thenReturn(IO(contentObjectResponse.copy(ref = id)))
    }

    (bitstreamInfo1 ++ bitstreamInfo2).foreach { bitstreamInfo =>
      when(
        preservicaClient
          .streamBitstreamContent(any[Fs2Streams[IO]])(any[String], any[Fs2Streams[IO]#BinaryStream => IO[Unit]])
      ).thenAnswer((_: Fs2Streams[IO], _: String, stream: Fs2Streams[IO]#BinaryStream => IO[Unit]) => {
        if (Option(stream).isDefined) {
          println("In Stream", s"File content for ${bitstreamInfo.name}".getBytes.length, s"File content for ${bitstreamInfo.name}")
          stream(Stream.emits(s"File content for ${bitstreamInfo.name}".getBytes)).unsafeRunSync()
        }
        IO.unit
      })
    }

    preservicaClient
  }

  private def runDisasterRecovery(
      sqsClient: DASQSClient[IO],
      preservicaClient: EntityClient[IO, Fs2Streams[IO]],
      repository: OcflRepository
  ): Unit = {
    val ocflService = new OcflService(repository)
    val processor = new Processor(config, sqsClient, ocflService, preservicaClient)
    Main.runDisasterRecovery(sqsClient, config, processor).compile.drain.unsafeRunSync()
  }

  private def metadataFile(id: UUID, ioType: String) = s"$id/${ioType.toUpperCase}_Metadata.xml"

  private def createExistingMetadataEntry(
      id: UUID,
      ioType: String,
      repo: OcflRepository,
      elem: Elem,
      destinationPath: String
  ) = {
    val existingMetadata = <AllMetadata>
      {elem}
    </AllMetadata>
    val xmlAsString = existingMetadata.toString()
    println("\n\n\ndestinationPath", destinationPath)
    addToRepo(id, repo, xmlAsString, metadataFile(id, ioType), destinationPath)
  }

  private def addToRepo(
      id: UUID,
      repo: OcflRepository,
      bodyAsString: String,
      sourceFilePath: String,
      destinationPath: String
  ) = {
    val path = Files.createTempDirectory(id.toString)
    Files.createDirectories(Paths.get(path.toString, id.toString))
    val fullSourceFilePath = Paths.get(path.toString, sourceFilePath)
    println("bodyAsString.getBytes", bodyAsString.getBytes.length, bodyAsString)
    Files.write(fullSourceFilePath, bodyAsString.getBytes)
    println("fullSourceFilePath, destinationPath", fullSourceFilePath, destinationPath)
    new OcflService(repo)
      .createObjects(List(IdWithSourceAndDestPaths(id, fullSourceFilePath, destinationPath)))
      .unsafeRunSync()
  }

  private def latestVersion(repo: OcflRepository, id: UUID): Long =
    repo.getObject(id.toHeadVersion).getObjectVersionId.getVersionNum.getVersionNum

  "runDisasterRecovery" should "write a new version and new IO metadata object, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val id = UUID.randomUUID()
      val repoDir = Files.createTempDirectory("repo")
      val repo = createTestRepo(repoDir)
      val sqsClient = mockSqs(InformationObjectMessage(id, s"$ioType:$id") :: Nil)
      val preservicaClient = mockPreservicaClient(elems = Seq(<Test></Test>))
      val expectedMetadataFileDestinationFilePath = metadataFile(id, ioType)

      runDisasterRecovery(sqsClient, preservicaClient, repo)

      latestVersion(repo, id) must equal(1)
      repo.containsObject(id.toString) must be(true)
      repo.getObject(id.toHeadVersion).containsFile(expectedMetadataFileDestinationFilePath) must be(true)

      println(repo.getObject(id.toHeadVersion).getFiles)

      val metadataStoragePath =
        repo.getObject(id.toHeadVersion).getFile(expectedMetadataFileDestinationFilePath).getStorageRelativePath
      val metadataContent = Files.readAllBytes(Paths.get(repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

      metadataContent must equal(
        <AllMetadata>
      <Test></Test>
    </AllMetadata>.toString
      )
    }

  "runDisasterRecovery" should "not write a new version, nor new IO metadata object if there is an IO update message with " +
    "the same metadata" in {
      val id = UUID.randomUUID()
      val sqsClient = mockSqs(InformationObjectMessage(id, s"$ioType:$id") :: Nil)
      val metadata = <Test></Test>
      val preservicaClient = mockPreservicaClient(elems = Seq(metadata))
      val repo = createTestRepo()
      val expectedVersionNumberBeforeAndAfter = 1
      createExistingMetadataEntry(id, ioType.toUpperCase, repo, metadata, metadataFile(id, ioType))

      latestVersion(repo, id) must equal(expectedVersionNumberBeforeAndAfter)
      runDisasterRecovery(sqsClient, preservicaClient, repo)

      latestVersion(repo, id) must equal(expectedVersionNumberBeforeAndAfter)
    }

  "runDisasterRecovery" should "write a new version and a new IO metadata object if there is an IO update with different metadata" in {
    val id = UUID.randomUUID()
    val sqsClient = mockSqs(InformationObjectMessage(id, s"$ioType:$id") :: Nil)
    val metadata = <Test></Test>
    val preservicaClient = mockPreservicaClient(elems = Seq(<DifferentMetadata></DifferentMetadata>))
    val repoDir = Files.createTempDirectory("repo")
    val repo = createTestRepo(repoDir)
    val expectedMetadataFileDestinationFilePath = metadataFile(id, ioType)
    createExistingMetadataEntry(id, ioType.toUpperCase, repo, metadata, expectedMetadataFileDestinationFilePath)

    println("getFiles1\n\n\n\n", repo.getObject(id.toHeadVersion).getFiles)
    latestVersion(repo, id) must equal(1)
    runDisasterRecovery(sqsClient, preservicaClient, repo)

    latestVersion(repo, id) must equal(2)

    println("expectedMetadataFileDestinationFilePath\n\n\n\n", expectedMetadataFileDestinationFilePath)
    println("getFiles2\n\n\n\n", repo.getObject(id.toHeadVersion).getFiles)

    val metadataStoragePath =
      repo.getObject(id.toHeadVersion).getFile(expectedMetadataFileDestinationFilePath).getStorageRelativePath
    val metadataContent = Files.readAllBytes(Paths.get(repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

    metadataContent must equal(
      <AllMetadata>
        <DifferentMetadata></DifferentMetadata>
      </AllMetadata>.toString
    )
  }

  "runDisasterRecovery" should "write multiple metadata fragments to the same file" in {
    val id = UUID.randomUUID()
    val repoDir = Files.createTempDirectory("repo")
    val repo = createTestRepo(repoDir)
    val sqsClient = mockSqs(InformationObjectMessage(id, s"$ioType:$id") :: Nil)
    val preservicaClient = mockPreservicaClient(elems = Seq(<Test1></Test1>, <Test2></Test2>, <Test3></Test3>))
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    val storagePath = repo.getObject(id.toHeadVersion).getFile(metadataFile(id, ioType)).getStorageRelativePath
    val xml = Files.readAllBytes(Paths.get(repoDir.toString, storagePath)).map(_.toChar).mkString
    val expectedXml = """<AllMetadata>
                        |      <Test1></Test1><Test2></Test2><Test3></Test3>
                        |    </AllMetadata>""".stripMargin
    xml must equal(expectedXml)
  }

  "runDisasterRecovery" should "only write one version if there are two identical IO messages" in {
    val id = UUID.randomUUID()
    val repo = createTestRepo()
    val sqsClient = mockSqs(List(InformationObjectMessage(id, s"io:$id"), InformationObjectMessage(id, s"io:$id")))
    val preservicaClient = mockPreservicaClient(elems = Seq(<Test></Test>))
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    latestVersion(repo, id) must equal(1)
  }

  "runDisasterRecovery" should "return an error if there is an error fetching the metadata" in {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val id = UUID.randomUUID()
    val repo = createTestRepo()
    val sqsClient = mockSqs(List(InformationObjectMessage(id, s"io:$id")))
    when(preservicaClient.metadataForEntity(any[Entity])).thenThrow(new Exception("Error getting metadata"))

    val ocflService = new OcflService(repo)
    val processor = new Processor(config, sqsClient, ocflService, preservicaClient)
    val err: Throwable =
      Main.runDisasterRecovery(sqsClient, config, processor).compile.drain.attempt.unsafeRunSync().left.value
    err.getMessage must equal("Error getting metadata")
  }

  "runDisasterRecovery" should "write a new version and a bitstream to a file, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val coId = UUID.randomUUID()
      val ioId = UUID.randomUUID()
      val metadata = <Test></Test>
      val repoDir = Files.createTempDirectory("repo")
      val repo = createTestRepo(repoDir)
      val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
      val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", ""), 1, Original, None, Some(ioId))
      val preservicaClient =
        mockPreservicaClient(ioId, coId, elems = Seq(metadata), bitstreamInfo1 = List(bitStreamInfo))
      val expectedCoFileDestinationFilePath = s"$ioId/Preservation_1/$coId/original/g1/name"
      createExistingMetadataEntry(
        ioId,
        ioType.toUpperCase,
        repo,
        metadata,
        s"$ioId/Preservation_1/$coId/CO_Metadata.xml"
      )

      println("\n\n\ngetFiles1", repo.getObject(ioId.toHeadVersion).getFiles)
      latestVersion(repo, ioId) must equal(1)
      runDisasterRecovery(sqsClient, preservicaClient, repo)

      latestVersion(repo, ioId) must equal(2)
      repo.containsObject(ioId.toString) must be(true)
      println("\n\n\ngetFiles2", repo.getObject(ioId.toHeadVersion).getFiles)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFileDestinationFilePath) must be(true)

      val coStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoFileDestinationFilePath).getStorageRelativePath
      val coContent = Files.readAllBytes(Paths.get(repoDir.toString, coStoragePath)).map(_.toChar).mkString

      coContent must equal(s"File content for ${bitStreamInfo.name}")
    }

  "runDisasterRecovery" should "not write a new version, nor a new bitstream if there is an CO update with the same bitstream" in {
    val data = "Test"
    val metadata = <Test></Test>
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val checksum = DigestUtils.sha256Hex(data)
    val repo = createTestRepo()
    addToRepo(ioId, repo, data, s"$ioId/name", s"$ioId/Preservation_1/$coId/original/g1/name")
    createExistingMetadataEntry(ioId, ioType.toUpperCase, repo, metadata, s"$ioId/Preservation_1/$coId/CO_Metadata.xml")
    val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
    val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", checksum), 1, Original, None, Some(ioId))
    val preservicaClient =
      mockPreservicaClient(ioId, coId, elems = Seq(metadata), bitstreamInfo1 = Seq(bitStreamInfo))
    val expectedVersionBeforeAndAfter = 2

    latestVersion(repo, ioId) must equal(expectedVersionBeforeAndAfter)
    runDisasterRecovery(sqsClient, preservicaClient, repo)

    latestVersion(repo, ioId) must equal(expectedVersionBeforeAndAfter)
  }

  "runDisasterRecovery" should "write a new version and a bitstream to a file, to the correct location in the repository, " +
    "if there is a CO update with different metadata" in {
      val data = "Test"
      val metadata = <Test></Test>
      val ioId = UUID.randomUUID()
      val coId = UUID.randomUUID()
      val repoDir = Files.createTempDirectory("repo")
      val repo = createTestRepo(repoDir)
      val expectedCoFileDestinationFilePath = s"$ioId/Preservation_1/$coId/original/g1/name"
      addToRepo(ioId, repo, data, s"$ioId/name", expectedCoFileDestinationFilePath)

      createExistingMetadataEntry(
        ioId,
        ioType.toUpperCase,
        repo,
        metadata,
        s"$ioId/Preservation_1/$coId/CO_Metadata.xml"
      )
      val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
      val checksum = DigestUtils.sha256Hex("DifferentData")
      val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", checksum), 1, Original, None, Some(ioId))
      val preservicaClient =
        mockPreservicaClient(ioId, coId, elems = Seq(metadata), bitstreamInfo1 = Seq(bitStreamInfo))

      latestVersion(repo, ioId) must equal(2)

      runDisasterRecovery(sqsClient, preservicaClient, repo)

      latestVersion(repo, ioId) must equal(3)
      repo.containsObject(ioId.toString) must be(true)

      println("getFiles\n\n\n", repo.getObject(ioId.toHeadVersion).getFiles)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFileDestinationFilePath) must be(true)

      val coStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoFileDestinationFilePath).getStorageRelativePath
      val coContent = Files.readAllBytes(Paths.get(repoDir.toString, coStoragePath)).map(_.toChar).mkString

      coContent must equal(s"File content for ${bitStreamInfo.name}")
    }

  "runDisasterRecovery" should "write multiple bitstreams to the same version and to the correct location" in {
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val coId2 = UUID.randomUUID()
    val coId3 = UUID.randomUUID()
    val metadata = <Test></Test>
    val repo = createTestRepo()
    val sqsClient = mockSqs(
      List(
        ContentObjectMessage(coId, s"co:$coId"),
        ContentObjectMessage(coId2, s"co:$coId2"),
        ContentObjectMessage(coId3, s"co:$coId3")
      )
    )
    val fixity = Fixity("SHA256", "")
    val bitStreamInfoList = Seq(
      BitStreamInfo("name1", 1, "", fixity, 1, Original, None, Some(ioId)),
      BitStreamInfo("name2", 1, "", fixity, 2, Derived, None, Some(ioId))
    )
    val bitStreamInfoList2 = Seq(
      BitStreamInfo("name3", 1, "", fixity, 1, Original, None, Some(ioId))
    )

    val preservicaClient = mockPreservicaClient(
      ioId,
      coId,
      coId2,
      coId3,
      elems = Seq(metadata),
      bitstreamInfo1 = bitStreamInfoList,
      bitstreamInfo2 = bitStreamInfoList2,
      addAccessRepUrl = true
    )

    createExistingMetadataEntry(ioId, ioType.toUpperCase, repo, metadata, s"$ioId/Preservation_1/$coId/CO_Metadata.xml")
    latestVersion(repo, ioId) must equal(1)
    runDisasterRecovery(sqsClient, preservicaClient, repo)

    val expectedCoFile1DestinationFilePath = s"$ioId/Preservation_1/$coId/original/g1/name1"
    val expectedCoFile2DestinationFilePath = s"$ioId/Preservation_1/$coId/derived/g2/name2"
    val expectedCoFile3DestinationFilePath = s"$ioId/Preservation_1/$coId2/original/g1/name3"
    val expectedCoFile4DestinationFilePath = s"$ioId/Access_1/$coId3/original/g1/name1"

    repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFile1DestinationFilePath) must be(true)
    repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFile2DestinationFilePath) must be(true)
    repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFile3DestinationFilePath) must be(true)
    repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFile4DestinationFilePath) must be(true)

    latestVersion(repo, ioId) must equal(2)
  }

  "runDisasterRecovery" should "only write one version if there are two identical CO messages" in {
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val repo = createTestRepo()
    val sqsClient = mockSqs(List(ContentObjectMessage(coId, s"co:$coId"), ContentObjectMessage(coId, s"co:$coId")))
    val preservicaClient = mockPreservicaClient(
      ioId,
      coId,
      bitstreamInfo1 = Seq(BitStreamInfo("name", 1, "", Fixity("SHA256", ""), 1, Original, None, Some(ioId)))
    )
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    latestVersion(repo, ioId) must equal(1)
  }

  "runDisasterRecovery" should "return an error if there is an error fetching the bitstream info" in {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val id = UUID.randomUUID()
    val repo = createTestRepo()
    val sqsClient = mockSqs(List(ContentObjectMessage(id, s"co:$id")))
    when(preservicaClient.getBitstreamInfo(any[UUID])).thenThrow(new Exception("Error getting bitstream info"))

    val ocflService = new OcflService(repo)
    val processor = new Processor(config, sqsClient, ocflService, preservicaClient)
    val err: Throwable =
      Main.runDisasterRecovery(sqsClient, config, processor).compile.drain.attempt.unsafeRunSync().left.value
    err.getMessage must equal("Error getting bitstream info")
  }

  "runDisasterRecovery" should "write a new version and new CO metadata object, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val data = "File content for name"
      val coId = UUID.randomUUID()
      val ioId = UUID.randomUUID()
      val metadata = <Test></Test>
      val repoDir = Files.createTempDirectory("repo")
      val repo = createTestRepo(repoDir)
      val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
      val checksum = DigestUtils.sha256Hex(data)
      val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", checksum), 1, Original, None, Some(ioId))
      val preservicaClient =
        mockPreservicaClient(ioId, coId, elems = Seq(metadata), bitstreamInfo1 = List(bitStreamInfo))
      val expectedMetadataFileDestinationFilePath = s"$ioId/Preservation_1/$coId/CO_Metadata.xml"

      addToRepo(ioId, repo, data, s"$ioId/name", s"$ioId/Preservation_1/$coId/original/g1/name")

      latestVersion(repo, ioId) must equal(1)
      runDisasterRecovery(sqsClient, preservicaClient, repo)

      latestVersion(repo, ioId) must equal(2)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedMetadataFileDestinationFilePath) must be(true)

      val metadataStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedMetadataFileDestinationFilePath).getStorageRelativePath
      val metadataContent = Files.readAllBytes(Paths.get(repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

      metadataContent must equal(
        <AllMetadata>
      <Test></Test>
    </AllMetadata>.toString
      )
    }

  "runDisasterRecovery" should "write a new version and new CO metadata object if there is a CO update with different metadata" in {
    val data = "Test"
    val metadata = <AllMetadata>
      <Test></Test>
    </AllMetadata>
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val checksum = DigestUtils.sha256Hex(data)
    val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
    val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", checksum), 1, Original, None, Some(ioId))
    val preservicaClient =
      mockPreservicaClient(
        ioId,
        coId,
        elems = Seq(<DifferentMetadata></DifferentMetadata>),
        bitstreamInfo1 = Seq(bitStreamInfo)
      )

    val repoDir = Files.createTempDirectory("repo")
    val repo = createTestRepo(repoDir)
    val expectedMetadataFileDestinationFilePath = s"$ioId/Preservation_1/$coId/CO_Metadata.xml"
    addToRepo(ioId, repo, data, s"$ioId/name", s"$ioId/Preservation_1/$coId/original/g1/name")
    createExistingMetadataEntry(ioId, ioType.toUpperCase, repo, metadata, expectedMetadataFileDestinationFilePath)

    latestVersion(repo, ioId) must equal(2)

    runDisasterRecovery(sqsClient, preservicaClient, repo)

    latestVersion(repo, ioId) must equal(3)

    println(".getFiles\n\n\n", repo.getObject(ioId.toHeadVersion).getFiles)

    val metadataStoragePath =
      repo.getObject(ioId.toHeadVersion).getFile(expectedMetadataFileDestinationFilePath).getStorageRelativePath
    val metadataContent = Files.readAllBytes(Paths.get(repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

    metadataContent must equal(
      <AllMetadata>
        <DifferentMetadata></DifferentMetadata>
      </AllMetadata>.toString
    )
  }

  "runDisasterRecovery" should "throw an error if the OCFL repository returns an unexpected error" in {
    val id = UUID.randomUUID()
    val repoDir = Files.createTempDirectory("repo")
    val sqsClient = mockSqs(InformationObjectMessage(id, s"$ioType:$id") :: Nil)
    val preservicaClient = mockPreservicaClient(elems = Seq(<Test></Test>))
    val expectedMetadataFileDestinationFilePath = metadataFile(id, ioType)

    val repo = mock[OcflRepository]
    when(repo.getObject(any[ObjectVersionId])).thenThrow(new Exception("Unexpected Exception"))

    val ex = intercept[Exception] {
      runDisasterRecovery(sqsClient, preservicaClient, repo)
    }
    ex.getMessage must equal(
      s"'getObject' returned an unexpected error 'java.lang.Exception: Unexpected Exception' when called with object id $id"
    )
  }
}
