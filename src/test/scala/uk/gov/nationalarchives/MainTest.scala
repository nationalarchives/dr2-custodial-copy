package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Decoder
import io.ocfl.api.model.DigestAlgorithm
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
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.EntityClient

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.compat.java8.FunctionConverters.asJavaConsumer
import scala.xml.Elem

class MainTest extends AnyFlatSpec with MockitoSugar with EitherValues {
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

  private def mockPreservicaMetadata(elems: Seq[Elem]): EntityClient[IO, Fs2Streams[IO]] = {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    when(preservicaClient.metadataForEntity(any[Entity])).thenReturn(IO(elems))
    preservicaClient
  }

  private def mockPreservicaBitstreams(
      ioId: UUID,
      coId: UUID,
      bitstreams: Seq[BitStreamInfo]
  ): EntityClient[IO, Fs2Streams[IO]] = {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    when(preservicaClient.getBitstreamInfo(ArgumentMatchers.eq(coId))).thenReturn(IO(bitstreams))
    when(preservicaClient.getEntity(coId, EntityClient.ContentObject))
      .thenReturn(
        fromType[IO](EntityClient.ContentObject.entityTypeShort, coId, None, None, false, parent = Option(ioId))
      )

    bitstreams.map { bitstream =>
      when(
        preservicaClient
          .streamBitstreamContent(any[Fs2Streams[IO]])(any[String], any[Fs2Streams[IO]#BinaryStream => IO[Unit]])
      ).thenAnswer((_: Fs2Streams[IO], _: String, stream: Fs2Streams[IO]#BinaryStream => IO[Unit]) => {
        if (Option(stream).isDefined) {
          stream(Stream.emits(bitstream.name.getBytes)).unsafeRunSync()
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

  private def metadataFile(id: UUID) = s"$id/tna-dr2-disaster-recovery-metadata.xml"

  private def createExistingMetadataEntry(id: UUID, repo: OcflRepository, elem: Elem) = {
    val existingMetadata = <AllMetadata>
      {elem}
    </AllMetadata>
    val xmlAsString = existingMetadata.toString()
    addToRepo(id, repo, xmlAsString, metadataFile(id))
  }

  private def addToRepo(id: UUID, repo: OcflRepository, bodyAsString: String, filePath: String) = {
    val path = Files.createTempDirectory(id.toString)
    Files.createDirectories(Paths.get(path.toString, id.toString))
    val fullFilePath = Paths.get(path.toString, filePath)
    Files.write(fullFilePath, bodyAsString.getBytes)
    new OcflService(repo).createObjects(List(IdWithPath(id, fullFilePath))).unsafeRunSync()
  }

  private def latestVersion(repo: OcflRepository, id: UUID): Long =
    repo.getObject(id.toHeadVersion).getObjectVersionId.getVersionNum.getVersionNum

  "runDisasterRecovery" should "write a new metadata object to the repository if it doesn't already exist" in {
    val id = UUID.randomUUID()
    val repo = createTestRepo()
    val sqsClient = mockSqs(InformationObjectMessage(id, s"io:$id") :: Nil)
    val preservicaClient = mockPreservicaMetadata(Seq(<Test></Test>))
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    repo.containsObject(id.toString) must be(true)
    repo.getObject(id.toHeadVersion).containsFile(metadataFile(id)) must be(true)
  }

  "runDisasterRecovery" should "not write a new version if there is an IO update message with the same metadata" in {
    val id = UUID.randomUUID()
    val sqsClient = mockSqs(InformationObjectMessage(id, s"io:$id") :: Nil)
    val metadata = <Test></Test>
    val preservicaClient = mockPreservicaMetadata(Seq(metadata))
    val repo = createTestRepo()
    createExistingMetadataEntry(id, repo, metadata)
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    latestVersion(repo, id) must equal(1)
  }

  "runDisasterRecovery" should "write a new version if there is an IO update with different metadata" in {
    val id = UUID.randomUUID()
    val sqsClient = mockSqs(InformationObjectMessage(id, s"io:$id") :: Nil)
    val metadata = <Test></Test>
    val preservicaClient = mockPreservicaMetadata(Seq(<DifferentMetadata></DifferentMetadata>))
    val repo = createTestRepo()
    createExistingMetadataEntry(id, repo, metadata)
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    latestVersion(repo, id) must equal(2)
  }

  "runDisasterRecovery" should "write multiple metadata fragments to the same file" in {
    val id = UUID.randomUUID()
    val repoDir = Files.createTempDirectory("repo")
    val repo = createTestRepo(repoDir)
    val sqsClient = mockSqs(InformationObjectMessage(id, s"io:$id") :: Nil)
    val preservicaClient = mockPreservicaMetadata(Seq(<Test1></Test1>, <Test2></Test2>, <Test3></Test3>))
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    val storagePath = repo.getObject(id.toHeadVersion).getFile(metadataFile(id)).getStorageRelativePath
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
    val preservicaClient = mockPreservicaMetadata(Seq(<Test></Test>))
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

  "runDisasterRecovery" should "write the bitstream to a file for a CO update" in {
    val coId = UUID.randomUUID()
    val ioId = UUID.randomUUID()
    val repoDir = Files.createTempDirectory("repo")
    val repo = createTestRepo(repoDir)
    val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
    val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", ""))
    val preservicaClient = mockPreservicaBitstreams(ioId, coId, List(bitStreamInfo))
    runDisasterRecovery(sqsClient, preservicaClient, repo)
    repo.containsObject(ioId.toString) must be(true)
    val filePath = s"$ioId/name"
    repo.getObject(ioId.toHeadVersion).containsFile(filePath) must be(true)
    val storagePath = repo.getObject(ioId.toHeadVersion).getFile(filePath).getStorageRelativePath
    val content = Files.readAllBytes(Paths.get(repoDir.toString, storagePath)).map(_.toChar).mkString
    content must equal(bitStreamInfo.name)
  }

  "runDisasterRecovery" should "not write a new version if there is an CO update with the same bitstream" in {
    val data = "Test"
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val checksum = DigestUtils.sha256Hex(data)
    val repo = createTestRepo()
    addToRepo(ioId, repo, data, s"$ioId/name")
    val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
    val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", checksum))
    val preservicaClient = mockPreservicaBitstreams(ioId, coId, Seq(bitStreamInfo))

    runDisasterRecovery(sqsClient, preservicaClient, repo)

    latestVersion(repo, ioId) must equal(1)
  }

  "runDisasterRecovery" should "write a new version if there is a CO update with different metadata" in {
    val data = "Test"
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val repo = createTestRepo()
    addToRepo(ioId, repo, data, s"$ioId/name")
    val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
    val checksum = DigestUtils.sha256Hex("DifferentData")
    val bitStreamInfo = BitStreamInfo("name", 1, "", Fixity("SHA256", checksum))
    val preservicaClient = mockPreservicaBitstreams(ioId, coId, Seq(bitStreamInfo))

    runDisasterRecovery(sqsClient, preservicaClient, repo)

    latestVersion(repo, ioId) must equal(2)
  }

  "runDisasterRecovery" should "write multiple bitstreams to the same version" in {
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val repo = createTestRepo()
    val sqsClient = mockSqs(ContentObjectMessage(coId, s"co:$coId") :: Nil)
    val fixity = Fixity("SHA256", "")
    val bitStreamInfoList = Seq(
      BitStreamInfo("name1", 1, "", fixity),
      BitStreamInfo("name2", 1, "", fixity)
    )
    val preservicaClient = mockPreservicaBitstreams(ioId, coId, bitStreamInfoList)

    runDisasterRecovery(sqsClient, preservicaClient, repo)

    repo.getObject(ioId.toHeadVersion).containsFile(s"$ioId/name1") must be(true)
    repo.getObject(ioId.toHeadVersion).containsFile(s"$ioId/name2") must be(true)
    latestVersion(repo, ioId) must equal(1)
  }

  "runDisasterRecovery" should "only write one version if there are two identical CO messages" in {
    val ioId = UUID.randomUUID()
    val coId = UUID.randomUUID()
    val repo = createTestRepo()
    val sqsClient = mockSqs(List(ContentObjectMessage(coId, s"co:$coId"), ContentObjectMessage(coId, s"co:$coId")))
    val preservicaClient = mockPreservicaBitstreams(ioId, coId, Seq(BitStreamInfo("name", 1, "", Fixity("SHA256", ""))))
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
}
