package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.ocfl.api.OcflRepository
import io.ocfl.api.model.ObjectVersionId
import org.apache.commons.codec.digest.DigestUtils
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.Main.*
import uk.gov.nationalarchives.OcflService.*
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.*
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils.MainTestUtils

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.xml.Utility.trim
import scala.xml.XML

class MainTest extends AnyFlatSpec with MockitoSugar with EitherValues {
  private def runDisasterRecovery(
      sqsClient: DASQSClient[IO],
      config: Config,
      processor: Processor
  ): Unit = Main.runDisasterRecovery(sqsClient, config, processor).compile.drain.unsafeRunSync()

  "runDisasterRecovery" should "write a new version and new IO metadata object, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val utils = new MainTestUtils(objectVersion = 0)
      val ioId = utils.ioId
      val repo = utils.repo
      val expectedIoMetadataFileDestinationPath = utils.expectedIoMetadataFileDestinationPath

      runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(1)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedIoMetadataFileDestinationPath) must be(true)

      val metadataStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedIoMetadataFileDestinationPath).getStorageRelativePath
      val metadataContent =
        Files.readAllBytes(Paths.get(utils.repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

      metadataContent must equal(
        <XIP xmlns="http://preservica.com/XIP/v6.9">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
        </XIP>.toString
      )
    }

  "runDisasterRecovery" should "not write a new version, nor new IO metadata object if there is an IO update message with " +
    "the same metadata" in {
      val utils = new MainTestUtils(typesOfMetadataFilesInRepo = List(InformationObject))
      val ioId = utils.ioId
      val repo = utils.repo

      runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(1)
    }

  "runDisasterRecovery" should "write a new version and a new IO metadata object if there is an IO update with different metadata" in {
    val utils = new MainTestUtils(
      typesOfMetadataFilesInRepo = List(InformationObject),
      metadataElemsPreservicaResponse = Seq(
        <Metadata><Ref/><Entity/><Content><differentThing xmlns="http://www.mockSchema.com/test/v42"></differentThing></Content></Metadata>
      )
    )
    val ioId = utils.ioId
    val repo = utils.repo

    runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

    utils.latestObjectVersion(repo, ioId) must equal(2)

    val metadataStoragePath =
      repo.getObject(ioId.toHeadVersion).getFile(utils.expectedIoMetadataFileDestinationPath).getStorageRelativePath
    val metadataContent =
      trim(
        XML.loadString(
          Files.readAllBytes(Paths.get(utils.repoDir.toString, metadataStoragePath)).map(_.toChar).mkString
        )
      )

    metadataContent must equal(
      trim(
        <XIP xmlns="http://preservica.com/XIP/v6.9">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Metadata><Ref/><Entity/><Content><differentThing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
        </XIP>
      )
    )
  }

  "runDisasterRecovery" should "write multiple metadata fragments to the same file" in {
    val utils =
      new MainTestUtils(
        objectVersion = 0,
        metadataElemsPreservicaResponse = Seq(
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>,
          <Metadata><Ref/><Entity/><Content><anotherThing xmlns="http://www.mockSchema.com/test/v42"></anotherThing></Content></Metadata>
        )
      )
    val ioId = utils.ioId
    val repo = utils.repo

    runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)
    utils.latestObjectVersion(repo, ioId) must equal(1)

    val metadataStoragePath =
      repo.getObject(ioId.toHeadVersion).getFile(utils.expectedIoMetadataFileDestinationPath).getStorageRelativePath
    val metadataContent =
      trim(
        XML.loadString(
          Files.readAllBytes(Paths.get(utils.repoDir.toString, metadataStoragePath)).map(_.toChar).mkString
        )
      )

    metadataContent must equal(
      trim(
        <XIP xmlns="http://preservica.com/XIP/v6.9">
                    <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
                    <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
                    <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
                    <Metadata><Ref/><Entity/><Content><anotherThing xmlns="http://www.mockSchema.com/test/v42"></anotherThing></Content></Metadata>
                  </XIP>
      )
    )
  }

  "runDisasterRecovery" should "only write one version if there are two identical IO messages" in {
    val utils = new MainTestUtils(List(InformationObject, InformationObject), objectVersion = 0)
    val ioId = utils.ioId
    val repo = utils.repo

    runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)
    utils.latestObjectVersion(repo, ioId) must equal(1)
  }

  "runDisasterRecovery" should "return an error if there is an error fetching the metadata" in {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val utils = new MainTestUtils(objectVersion = 0)
    when(preservicaClient.metadataForEntity(any[Entity])).thenThrow(new RuntimeException("Error getting metadata"))

    val processor =
      new Processor(utils.config, utils.sqsClient, utils.ocflService, preservicaClient, utils.xmlValidator)
    val err: Throwable =
      Main
        .runDisasterRecovery(utils.sqsClient, utils.config, processor)
        .compile
        .drain
        .attempt
        .unsafeRunSync()
        .left
        .value
    err.getMessage must equal("Error getting metadata")
  }

  "runDisasterRecovery" should "return an error if a CO has no parent" in {
    val bitstreamInfo = Seq(BitStreamInfo("name1", 1, "", Fixity("SHA256", ""), 1, Original, None, None))
    val utils = new MainTestUtils(List(ContentObject), 0, bitstreamInfo1Responses = bitstreamInfo)

    val err: Throwable =
      Main
        .runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)
        .compile
        .drain
        .attempt
        .unsafeRunSync()
        .left
        .value
    err.getMessage must equal("Cannot get IO reference from CO")
  }

  "runDisasterRecovery" should "return an error if a CO belongs to more than one Representation type" in {
    val ioId = UUID.randomUUID()
    val utils = new MainTestUtils(
      List(ContentObject),
      0,
      bitstreamInfo2Responses = Seq(BitStreamInfo("name1", 1, "", Fixity("SHA256", ""), 1, Original, None, Some(ioId))),
      addAccessRepUrl = true
    )
    val coId = utils.coId1

    val err: Throwable =
      Main
        .runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)
        .compile
        .drain
        .attempt
        .unsafeRunSync()
        .left
        .value
    err.getMessage must equal(
      s"$coId belongs to more than 1 representation type: Preservation_1, Access_1"
    )
  }

  "runDisasterRecovery" should "write a new version and a bitstream to a file, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val utils = new MainTestUtils(
        List(ContentObject),
        typesOfMetadataFilesInRepo = List(ContentObject)
      )
      val repo = utils.repo
      val ioId = utils.ioId

      val expectedCoFileDestinationFilePath = utils.expectedCoFileDestinationPath

      runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(2)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFileDestinationFilePath) must be(true)

      val coStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoFileDestinationFilePath).getStorageRelativePath
      val coContent = Files.readAllBytes(Paths.get(utils.repoDir.toString, coStoragePath)).map(_.toChar).mkString

      coContent must equal(s"File content for name1")
    }

  "runDisasterRecovery" should "not write a new version, nor a new bitstream if there is an CO update with the same bitstream" in {
    val ioId = UUID.randomUUID()
    val fileContent = "Test"
    val checksum = DigestUtils.sha256Hex(fileContent)
    val expectedVersionBeforeAndAfter = 2
    val utils = new MainTestUtils(
      List(ContentObject),
      expectedVersionBeforeAndAfter,
      typesOfMetadataFilesInRepo = List(ContentObject),
      fileContentToWriteToEachFileInRepo = List(fileContent),
      bitstreamInfo1Responses =
        List(BitStreamInfo("name1", 1, "", Fixity("SHA256", checksum), 1, Original, None, Some(ioId)))
    )
    val repo = utils.repo

    runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

    utils.latestObjectVersion(repo, ioId) must equal(expectedVersionBeforeAndAfter)
  }

  "runDisasterRecovery" should "write a new version and a bitstream to a file, to the correct location in the repository, " +
    "if there is a CO update with different metadata" in {
      val fileContent = "Test"
      val ioId = UUID.randomUUID()

      val utils = new MainTestUtils(
        List(ContentObject),
        2,
        typesOfMetadataFilesInRepo = List(ContentObject),
        fileContentToWriteToEachFileInRepo = List(fileContent),
        bitstreamInfo1Responses =
          List(BitStreamInfo("name1", 1, "", Fixity("SHA256", "DifferentContent"), 1, Original, None, Some(ioId)))
      )

      val repo = utils.repo
      val expectedCoFileDestinationFilePath = utils.expectedCoFileDestinationPath

      runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(3)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFileDestinationFilePath) must be(true)

      val coStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoFileDestinationFilePath).getStorageRelativePath
      val coContent = Files.readAllBytes(Paths.get(utils.repoDir.toString, coStoragePath)).map(_.toChar).mkString

      coContent must equal(s"File content for name1")
    }

  "runDisasterRecovery" should "write multiple bitstreams to the same version and to the correct location" in {
    val ioId = UUID.randomUUID()
    val fixity = Fixity("SHA256", "")
    val bitStreamInfoList = Seq(
      BitStreamInfo("name1", 1, "", fixity, 1, Original, None, Some(ioId)),
      BitStreamInfo("name2", 1, "", fixity, 2, Derived, None, Some(ioId))
    )
    val bitStreamInfoList2 = Seq(
      BitStreamInfo("name3", 1, "", fixity, 1, Original, None, Some(ioId))
    )

    val utils = new MainTestUtils(
      List(ContentObject, ContentObject, ContentObject),
      typesOfMetadataFilesInRepo = List(ContentObject),
      bitstreamInfo1Responses = bitStreamInfoList,
      bitstreamInfo2Responses = bitStreamInfoList2,
      addAccessRepUrl = true
    )
    val coId = utils.coId1
    val coId2 = utils.coId2
    val coId3 = utils.coId3
    val repo = utils.repo

    runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

    val expectedCoFileDestinationFilePaths = List(
      s"$ioId/Preservation_1/$coId/original/g1/name1",
      s"$ioId/Preservation_1/$coId/derived/g2/name2",
      s"$ioId/Preservation_1/$coId2/original/g1/name3",
      s"$ioId/Access_1/$coId3/original/g1/name1"
    )
    expectedCoFileDestinationFilePaths.foreach { path =>
      repo.getObject(ioId.toHeadVersion).containsFile(path) must be(true)
    }

    utils.latestObjectVersion(repo, ioId) must equal(2)
  }

  "runDisasterRecovery" should "only write one version if there are two identical CO messages" in {
    val utils = new MainTestUtils(List(ContentObject), 0)
    val ioId = utils.ioId
    val repo = utils.repo

    runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)
    utils.latestObjectVersion(repo, ioId) must equal(1)
  }

  "runDisasterRecovery" should "return an error if there is an error fetching the bitstream info" in {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val utils = new MainTestUtils(List(ContentObject), 0)
    val sqsClient = utils.sqsClient
    when(preservicaClient.getBitstreamInfo(any[UUID])).thenThrow(new RuntimeException("Error getting bitstream info"))

    val processor = new Processor(utils.config, sqsClient, utils.ocflService, preservicaClient, utils.xmlValidator)
    val err: Throwable =
      Main.runDisasterRecovery(sqsClient, utils.config, processor).compile.drain.attempt.unsafeRunSync().left.value
    err.getMessage must equal("Error getting bitstream info")
  }

  "runDisasterRecovery" should "write a new version and new CO metadata object, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val fileContent = "File content for name"
      val checksum = DigestUtils.sha256Hex(fileContent)
      val ioId = UUID.randomUUID()
      val utils = new MainTestUtils(
        List(ContentObject),
        1,
        fileContentToWriteToEachFileInRepo = List(fileContent),
        bitstreamInfo1Responses =
          List(BitStreamInfo("name1", 1, "", Fixity("SHA256", checksum), 1, Original, None, Some(ioId)))
      )
      val repo = utils.repo
      val expectedCoMetadataFileDestinationPath = utils.expectedCoMetadataFileDestinationPath

      runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(2)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoMetadataFileDestinationPath) must be(true)

      val metadataStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoMetadataFileDestinationPath).getStorageRelativePath
      val metadataContent =
        Files.readAllBytes(Paths.get(utils.repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

      metadataContent must equal(
        <XIP xmlns="http://preservica.com/XIP/v6.9">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
        </XIP>.toString
      )
    }

  "runDisasterRecovery" should "write a new version and new CO metadata object if there is a CO update with different metadata" in {
    val fileContent = "Test"
    val checksum = DigestUtils.sha256Hex(fileContent)
    val ioId = UUID.randomUUID()
    val utils = new MainTestUtils(
      List(InformationObject),
      2,
      typesOfMetadataFilesInRepo = List(ContentObject),
      fileContentToWriteToEachFileInRepo = List(fileContent),
      metadataElemsPreservicaResponse = Seq(
        <Metadata><Ref/><Entity/><Content><differentThing xmlns="http://www.mockSchema.com/test/v42"></differentThing></Content></Metadata>
      ),
      bitstreamInfo1Responses =
        List(BitStreamInfo("name1", 1, "", Fixity("SHA256", checksum), 1, Original, None, Some(ioId)))
    )
    val repo = utils.repo

    runDisasterRecovery(utils.sqsClient, utils.config, utils.processor)

    utils.latestObjectVersion(repo, ioId) must equal(3)

    val metadataStoragePath =
      repo.getObject(ioId.toHeadVersion).getFile(utils.expectedCoMetadataFileDestinationPath).getStorageRelativePath
    val metadataContent =
      trim(
        XML.loadString(
          Files.readAllBytes(Paths.get(utils.repoDir.toString, metadataStoragePath)).map(_.toChar).mkString
        )
      )

    metadataContent must equal(
      trim(
        <XIP xmlns="http://preservica.com/XIP/v6.9">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
        </XIP>
      )
    )
  }

  "runDisasterRecovery" should "throw an error if the OCFL repository returns an unexpected error" in {
    val utils = new MainTestUtils(objectVersion = 0)
    val ioId = utils.ioId

    val repo = mock[OcflRepository]
    when(repo.getObject(any[ObjectVersionId])).thenThrow(new RuntimeException("Unexpected Exception"))

    val ocflService = new OcflService(repo)
    val processor =
      new Processor(utils.config, utils.sqsClient, ocflService, utils.preservicaClient, utils.xmlValidator)

    val ex = intercept[Exception] {
      runDisasterRecovery(utils.sqsClient, utils.config, processor)
    }
    ex.getMessage must equal(
      s"'getObject' returned an unexpected error 'java.lang.RuntimeException: Unexpected Exception' when called with object id $ioId"
    )
  }
}
