package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import io.circe.Decoder
import io.ocfl.api.MutableOcflRepository
import io.ocfl.api.model.ObjectVersionId
import org.apache.commons.codec.digest.DigestUtils
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito.{never, reset, times, verify, when}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.*
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.custodialcopy.Main.*
import uk.gov.nationalarchives.custodialcopy.Message.{ReceivedSnsMessage, SoReceivedSnsMessage}
import uk.gov.nationalarchives.custodialcopy.Processor.Result
import uk.gov.nationalarchives.utils.TestUtils.*
import uk.gov.nationalarchives.custodialcopy.testUtils.ExternalServicesTestUtils.MainTestUtils
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.*

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.xml.Utility.trim
import scala.xml.XML

class MainTest extends AnyFlatSpec with MockitoSugar with EitherValues {

  private def getError(
      sqsClient: DASQSClient[IO],
      config: Config,
      processor: Processor
  ): Throwable = runCustodialCopy(sqsClient, config, processor).head match
    case Result.Failure(e) => e
    case _                 => throw new Exception("Expected an error but none found")

  private def runCustodialCopy(
      sqsClient: DASQSClient[IO],
      config: Config,
      processor: Processor
  ): List[Result] = Main.runCustodialCopy(sqsClient, config, processor).compile.toList.unsafeRunSync().flatten

  private val exampleUrl = "https://example.com"

  "runCustodialCopy" should "(given an IO message with 'deleted' set to 'true') delete all objects underneath it" in {
    val fixity = List(Fixity("SHA256", ""))
    val ioId = UUID.randomUUID()
    val bitStreamInfoList = Seq(
      BitStreamInfo("90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt", 1, "", fixity, 1, Original, None, Some(ioId)),
      BitStreamInfo("90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt2", 1, "", fixity, 2, Derived, None, Some(ioId))
    )
    val bitStreamInfoList2 = Seq(
      BitStreamInfo("90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt3", 1, "", fixity, 1, Original, None, Some(ioId))
    )
    val utils = new MainTestUtils(
      List((ContentObject, false), (InformationObject, true)),
      typesOfMetadataFilesInRepo = List(InformationObject, ContentObject),
      objectVersion = 2,
      fileContentToWriteToEachFileInRepo = List("fileContent1", "fileContent2"),
      entityDeleted = true,
      bitstreamInfo1Responses = bitStreamInfoList,
      bitstreamInfo2Responses = bitStreamInfoList2,
      addAccessRepUrl = true
    )

    val repo = utils.repo
    val ioMetadataDestinationPath = s"$ioId/IO_Metadata.xml"
    val expectedDestinationFilePathsAlreadyInRepo = List(
      s"$ioId/Preservation_1/${utils.coId1}/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
      s"$ioId/Preservation_1/${utils.coId1}/derived/g2/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt2",
      s"$ioId/Preservation_1/${utils.coId1}/CO_Metadata.xml",
      ioMetadataDestinationPath
    )

    expectedDestinationFilePathsAlreadyInRepo.foreach { path =>
      repo.getObject(ioId.toHeadVersion).containsFile(path) must be(true)
    }
    utils.latestObjectVersion(repo, utils.ioId) must equal(2)

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

    val expectedDestinationFilePathsRemovedFromRepo = List(
      s"$ioId/Preservation_1/${utils.coId2}/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt3",
      s"$ioId/Access_1/${utils.coId3}/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt"
    ) ++ expectedDestinationFilePathsAlreadyInRepo

    val ocflObject = repo.getObject(ioId.toHeadVersion)

    repo.getObject(ioId.toHeadVersion).getFiles.toArray.toList must be(Nil)
    utils.latestObjectVersion(repo, utils.ioId) must equal(2)
  }

  "runCustodialCopy" should "write a new version and new IO metadata object, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val utils = new MainTestUtils(
        List((InformationObject, false)),
        bitstreamInfo2Responses = Seq(
          BitStreamInfo(
            "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt2",
            1,
            "",
            List(Fixity("SHA256", "")),
            1,
            Original,
            None,
            Some(UUID.randomUUID())
          )
        ),
        objectVersion = 0
      )
      val ioId = utils.ioId
      val repo = utils.repo
      val expectedIoMetadataFileDestinationPath = utils.expectedIoMetadataFileDestinationPath

      runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(2) // The OCFL library generates an empty v1 when you use the mutable repository.
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedIoMetadataFileDestinationPath) must be(true)

      val metadataStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedIoMetadataFileDestinationPath).getStorageRelativePath
      val metadataContent =
        Files.readAllBytes(Paths.get(utils.repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

      metadataContent must equal(
        <XIP xmlns="http://preservica.com/XIP/v7.7">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Representation><InformationObject/><Name/><Type/><ContentObjects><ContentObject/></ContentObjects><RepresentationFormats/><RepresentationProperties/></Representation>
          <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>
          <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>.toString
      )
    }

  "runCustodialCopy" should "delete all SO messages it receives" in {
    val utils = new MainTestUtils(typesOfSqsMsgAndDeletionStatus = List((StructuralObject, false), (StructuralObject, false)), objectVersion = 0)

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

    verify(utils.sqsClient, times(4)).deleteMessage(any[String], any[String])
  }

  "runCustodialCopy" should "not write a new version, nor new IO metadata object if there is an IO message with " +
    "the same metadata" in {
      val utils = new MainTestUtils(typesOfMetadataFilesInRepo = List(InformationObject))
      val ioId = utils.ioId
      val repo = utils.repo

      runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(2)
    }

  "runCustodialCopy" should "write a new version and a new IO metadata object if there is an IO message with different metadata" in {
    val utils = new MainTestUtils(
      typesOfMetadataFilesInRepo = List(InformationObject),
      metadataElemsPreservicaResponse = Seq(
        <Metadata><Ref/><Entity/><Content><differentThing xmlns="http://www.mockSchema.com/test/v42"></differentThing></Content></Metadata>
      )
    )
    val ioId = utils.ioId
    val repo = utils.repo

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

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
        <XIP xmlns="http://preservica.com/XIP/v7.7">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Representation><InformationObject/><Name/><Type/><ContentObjects><ContentObject/></ContentObjects><RepresentationFormats/><RepresentationProperties/></Representation>
          <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>
          <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><differentThing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>
      )
    )
  }

  "runCustodialCopy" should "write multiple metadata fragments to the same file" in {
    val utils =
      new MainTestUtils(
        objectVersion = 0,
        metadataElemsPreservicaResponse = Seq(
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>,
          <Metadata><Ref/><Entity/><Content><anotherThing xmlns="http://www.mockSchema.com/test/v42"></anotherThing></Content></Metadata>
        ),
        bitstreamInfo2Responses = Seq(
          BitStreamInfo(
            "90dfb573-7419-4e89-8558-6cfa29f8fb16.testEx2",
            1,
            "",
            List(Fixity("SHA256", "")),
            1,
            Original,
            None,
            Some(UUID.randomUUID())
          )
        )
      )
    val ioId = utils.ioId
    val repo = utils.repo

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)
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
        <XIP xmlns="http://preservica.com/XIP/v7.7">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Representation><InformationObject/><Name/><Type/><ContentObjects><ContentObject/></ContentObjects><RepresentationFormats/><RepresentationProperties/></Representation>
          <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>
          <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
          <Metadata><Ref/><Entity/><Content><anotherThing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
                  </XIP>
      )
    )
  }

  "runCustodialCopy" should "only write one version if there are two identical IO messages" in {
    val utils = new MainTestUtils(
      List((InformationObject, false), (InformationObject, false)),
      bitstreamInfo2Responses = Seq(
        BitStreamInfo(
          "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt2",
          1,
          "",
          List(Fixity("SHA256", "")),
          1,
          Original,
          None,
          Some(UUID.randomUUID())
        )
      ),
      objectVersion = 0
    )
    val ioId = utils.ioId
    val repo = utils.repo

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)
    utils.latestObjectVersion(repo, ioId) must equal(2)
    verify(utils.sqsClient, times(4)).deleteMessage(any[String], any[String])
  }

  "runCustodialCopy" should "return an error if there is an error fetching the metadata" in {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val utils = new MainTestUtils(objectVersion = 0)
    when(preservicaClient.metadataForEntity(any[Entity])).thenThrow(new RuntimeException("Error getting metadata"))

    val processor =
      new Processor(
        utils.config,
        utils.sqsClient,
        utils.ocflService,
        preservicaClient,
        utils.xmlValidator,
        utils.snsClient
      )
    val err: Throwable = getError(utils.sqsClient, utils.config, processor)

    err.getMessage must equal("Error getting metadata")
  }

  "runCustodialCopy" should "not call the process method if no messages are received" in {
    val processor = mock[Processor]
    when(processor.process(any[MessageResponse[ReceivedSnsMessage]])).thenReturn(IO.unit)
    val utils = new MainTestUtils(typesOfSqsMsgAndDeletionStatus = Nil, objectVersion = 0)

    runCustodialCopy(utils.sqsClient, utils.config, processor)

    verify(processor, never()).process(any[MessageResponse[ReceivedSnsMessage]])
  }

  "runCustodialCopy" should "(given a CO message with 'deleted' set to 'true') throw an Exception" in {
    val fixity = Fixity("SHA256", "")

    val utils = new MainTestUtils(
      List((ContentObject, true)),
      typesOfMetadataFilesInRepo = List(InformationObject, ContentObject),
      objectVersion = 2,
      fileContentToWriteToEachFileInRepo = List("fileContent1"),
      entityDeleted = true
    )

    val repo = utils.repo

    utils.latestObjectVersion(repo, utils.ioId) must equal(2)

    val err: Throwable = getError(utils.sqsClient, utils.config, utils.processor)

    err.getMessage must equal(s"A Content Object '${utils.coId1}' has been deleted in Preservica")
  }

  "runCustodialCopy" should "(given a CO and IO message that both have 'deleted' set to 'true') the exception message thrown by the CO message " +
    "doesn't prevent the IO message from being processed" in {
      val fixity = Fixity("SHA256", "")

      val utils = new MainTestUtils(
        List((ContentObject, true), (InformationObject, true)),
        typesOfMetadataFilesInRepo = List(InformationObject, ContentObject),
        objectVersion = 2,
        fileContentToWriteToEachFileInRepo = List("fileContent1"),
        entityDeleted = true
      )

      val repo = utils.repo

      utils.latestObjectVersion(repo, utils.ioId) must equal(2)
      val expectedDestinationFilePathsAlreadyInRepo = List(
        s"${utils.ioId}/Preservation_1/${utils.coId1}/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
        s"${utils.ioId}/Preservation_1/${utils.coId1}/CO_Metadata.xml",
        s"${utils.ioId}/IO_Metadata.xml"
      )

      expectedDestinationFilePathsAlreadyInRepo.foreach { path =>
        repo.getObject(utils.ioId.toHeadVersion).containsFile(path) must be(true)
      }

      val err: Throwable = getError(utils.sqsClient, utils.config, utils.processor)

      err.getMessage must equal(s"A Content Object '${utils.coId1}' has been deleted in Preservica")
      utils.latestObjectVersion(repo, utils.ioId) must equal(2)
      repo.getObject(utils.ioId.toHeadVersion).getFiles.toArray.toList must be(Nil)
    }

  "runCustodialCopy" should "(given a CO message that has 'deleted' set to 'false' and IO message that has 'deleted' set to 'true') " +
    "parse the non-deleted (CO) message first" in {
      val fixity = Fixity("SHA256", "")

      val utils = new MainTestUtils(
        List((ContentObject, false), (InformationObject, true)),
        typesOfMetadataFilesInRepo = List(InformationObject, ContentObject),
        objectVersion = 2,
        fileContentToWriteToEachFileInRepo = List("fileContent1"),
        entityDeleted = true
      )

      val repo = utils.repo

      utils.latestObjectVersion(repo, utils.ioId) must equal(2)
      val expectedDestinationFilePathsAlreadyInRepo = List(
        s"${utils.ioId}/Preservation_1/${utils.coId1}/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
        s"${utils.ioId}/Preservation_1/${utils.coId1}/CO_Metadata.xml",
        s"${utils.ioId}/IO_Metadata.xml"
      )

      expectedDestinationFilePathsAlreadyInRepo.foreach { path =>
        repo.getObject(utils.ioId.toHeadVersion).containsFile(path) must be(true)
      }

      runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, utils.ioId) must equal(2)

      repo.getObject(utils.ioId.toHeadVersion).getFiles.toArray.toList must be(Nil)
    }

  "runCustodialCopy" should "return an error if a CO has no parent" in {
    val bitstreamInfo = Seq(
      BitStreamInfo(
        "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
        1,
        "",
        List(Fixity("SHA256", "")),
        1,
        Original,
        None,
        None
      )
    )
    val utils = new MainTestUtils(List((ContentObject, false)), 0, bitstreamInfo1Responses = bitstreamInfo)

    val err: Throwable = getError(utils.sqsClient, utils.config, utils.processor)
    err.getMessage must equal("Cannot get IO reference from CO")
  }

  "runCustodialCopy" should "return an error if a CO belongs to more than one Representation type" in {
    val ioId = UUID.randomUUID()
    val utils = new MainTestUtils(
      List((ContentObject, false)),
      0,
      bitstreamInfo2Responses = Seq(
        BitStreamInfo(
          "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          1,
          "",
          List(Fixity("SHA256", "")),
          1,
          Original,
          None,
          Some(ioId)
        )
      ),
      addAccessRepUrl = true
    )
    val coId = utils.coId1

    val err: Throwable = getError(utils.sqsClient, utils.config, utils.processor)

    err.getMessage must equal(
      s"$coId belongs to more than 1 representation type: Preservation_1, Access_1"
    )
  }

  "runCustodialCopy" should "write a new version and a bitstream to a file, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val utils = new MainTestUtils(
        List((ContentObject, false)),
        typesOfMetadataFilesInRepo = List(ContentObject)
      )
      val repo = utils.repo
      val ioId = utils.ioId

      val expectedCoFileDestinationFilePath = utils.expectedCoFileDestinationPath

      runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(2)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFileDestinationFilePath) must be(true)

      val coStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoFileDestinationFilePath).getStorageRelativePath
      val coContent = Files.readAllBytes(Paths.get(utils.repoDir.toString, coStoragePath)).map(_.toChar).mkString

      coContent must equal(s"File content for 90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt")
    }

  "runCustodialCopy" should "not write a new version, nor a new bitstream if there is an CO message with the same bitstream" in {
    val ioId = UUID.randomUUID()
    val fileContent = "Test"
    val checksum = DigestUtils.sha256Hex(fileContent)
    val expectedVersionBeforeAndAfter = 2
    val utils = new MainTestUtils(
      List((ContentObject, false)),
      expectedVersionBeforeAndAfter,
      typesOfMetadataFilesInRepo = List(ContentObject),
      fileContentToWriteToEachFileInRepo = List(fileContent),
      bitstreamInfo1Responses = List(
        BitStreamInfo(
          "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          1,
          "",
          List(Fixity("SHA256", checksum)),
          1,
          Original,
          None,
          Some(ioId)
        )
      )
    )
    val repo = utils.repo

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

    utils.latestObjectVersion(repo, ioId) must equal(expectedVersionBeforeAndAfter)
  }

  "runCustodialCopy" should "write a new version and a bitstream to a file, to the correct location in the repository, " +
    "if there is a CO message with different metadata" in {
      val fileContent = "Test"
      val ioId = UUID.randomUUID()

      val utils = new MainTestUtils(
        List((ContentObject, false)),
        2,
        typesOfMetadataFilesInRepo = List(ContentObject),
        fileContentToWriteToEachFileInRepo = List(fileContent),
        bitstreamInfo1Responses = List(
          BitStreamInfo(
            "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
            1,
            exampleUrl,
            List(Fixity("SHA256", "DifferentContent")),
            1,
            Original,
            None,
            Some(ioId)
          )
        )
      )

      val repo = utils.repo
      val expectedCoFileDestinationFilePath = utils.expectedCoFileDestinationPath

      runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(2)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFileDestinationFilePath) must be(true)

      val coStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoFileDestinationFilePath).getStorageRelativePath
      val coContent = Files.readAllBytes(Paths.get(utils.repoDir.toString, coStoragePath)).map(_.toChar).mkString

      coContent must equal(s"File content for 90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt")
    }

  "runCustodialCopy" should "not write a new version and a bitstream to a file if the CO bitstream has no url" in {
    val fileContent = "Test"
    val ioId = UUID.randomUUID()

    val utils = new MainTestUtils(
      List((ContentObject, false)),
      0,
      bitstreamInfo1Responses = List(
        BitStreamInfo(
          "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          1,
          "",
          List(Fixity("SHA256", "DifferentContent")),
          1,
          Original,
          None,
          Some(ioId)
        )
      )
    )

    val repo = utils.repo
    val expectedCoFileDestinationFilePath = utils.expectedCoFileDestinationPath

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

    utils.latestObjectVersion(repo, ioId) must equal(2)
    repo.containsObject(ioId.toString) must be(true)
    repo.getObject(ioId.toHeadVersion).containsFile(expectedCoFileDestinationFilePath) must be(false)

    repo.getObject(ioId.toHeadVersion).getFile(expectedCoFileDestinationFilePath) must equal(null)
  }

  "runCustodialCopy" should "write multiple bitstreams to the same version and to the correct location" in {
    val ioId = UUID.randomUUID()
    val fixity = List(Fixity("SHA256", ""))
    val bitStreamInfoList = Seq(
      BitStreamInfo("90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt", 1, exampleUrl, fixity, 1, Original, None, Some(ioId)),
      BitStreamInfo("90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt2", 1, exampleUrl, fixity, 2, Derived, None, Some(ioId))
    )
    val bitStreamInfoList2 = Seq(
      BitStreamInfo("90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt3", 1, exampleUrl, fixity, 1, Original, None, Some(ioId))
    )

    val utils = new MainTestUtils(
      List((ContentObject, false), (ContentObject, false), (ContentObject, false)),
      typesOfMetadataFilesInRepo = List(ContentObject),
      bitstreamInfo1Responses = bitStreamInfoList,
      bitstreamInfo2Responses = bitStreamInfoList2,
      addAccessRepUrl = true
    )
    val coId = utils.coId1
    val coId2 = utils.coId2
    val coId3 = utils.coId3
    val repo = utils.repo

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

    val expectedCoFileDestinationFilePaths = List(
      s"$ioId/Preservation_1/$coId/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
      s"$ioId/Preservation_1/$coId/derived/g2/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt2",
      s"$ioId/Preservation_1/$coId2/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt3",
      s"$ioId/Access_1/$coId3/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt"
    )

    expectedCoFileDestinationFilePaths.foreach { path =>
      repo.getObject(ioId.toHeadVersion).containsFile(path) must be(true)
    }

    utils.latestObjectVersion(repo, ioId) must equal(2)
  }

  "runCustodialCopy" should "only write one version if there are two identical CO messages" in {
    val utils = new MainTestUtils(List((ContentObject, false)), 0)
    val ioId = utils.ioId
    val repo = utils.repo

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)
    utils.latestObjectVersion(repo, ioId) must equal(2)
  }

  "runCustodialCopy" should "return an error if there is an error fetching the bitstream info" in {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val utils = new MainTestUtils(List((ContentObject, false)), 0)
    val sqsClient = utils.sqsClient
    when(preservicaClient.getBitstreamInfo(any[UUID])).thenThrow(new RuntimeException("Error getting bitstream info"))

    val processor =
      new Processor(utils.config, sqsClient, utils.ocflService, preservicaClient, utils.xmlValidator, utils.snsClient)

    val err: Throwable = getError(sqsClient, utils.config, processor)
    err.getMessage must equal("Error getting bitstream info")
  }

  "runCustodialCopy" should "write a new version and new CO metadata object, to the correct location in the repository " +
    "if it doesn't already exist" in {
      val fileContent = "File content for name"
      val checksum = DigestUtils.sha256Hex(fileContent)
      val ioId = UUID.randomUUID()
      val utils = new MainTestUtils(
        List((ContentObject, false)),
        2,
        fileContentToWriteToEachFileInRepo = List(fileContent),
        bitstreamInfo1Responses = List(
          BitStreamInfo(
            "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
            1,
            "",
            List(Fixity("SHA256", checksum)),
            1,
            Original,
            None,
            Some(ioId)
          )
        )
      )
      val repo = utils.repo
      val expectedCoMetadataFileDestinationPath = utils.expectedCoMetadataFileDestinationPath

      runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

      utils.latestObjectVersion(repo, ioId) must equal(2)
      repo.containsObject(ioId.toString) must be(true)
      repo.getObject(ioId.toHeadVersion).containsFile(expectedCoMetadataFileDestinationPath) must be(true)

      val metadataStoragePath =
        repo.getObject(ioId.toHeadVersion).getFile(expectedCoMetadataFileDestinationPath).getStorageRelativePath
      val metadataContent =
        Files.readAllBytes(Paths.get(utils.repoDir.toString, metadataStoragePath)).map(_.toChar).mkString

      metadataContent must equal(
        <XIP xmlns="http://preservica.com/XIP/v7.7">
          <ContentObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></ContentObject>
          <Generation original="true" active="true"><ContentObject>someContent</ContentObject></Generation>
          <Bitstream><Filename>90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt</Filename><FileSize>1</FileSize><Fixities><Fixity><FixityAlgorithmRef>SHA256</FixityAlgorithmRef><FixityValue>a3f79ff30708357d9b94d2e11507a9b30bef88d816bfffdc0ec3136939289ff3</FixityValue></Fixity></Fixities></Bitstream>
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>.toString
      )
    }

  "runCustodialCopy" should "write a new version and new CO metadata object if there is a CO message with different metadata" in {
    val fileContent = "Test"
    val checksum = DigestUtils.sha256Hex(fileContent)
    val ioId = UUID.randomUUID()
    val utils = new MainTestUtils(
      List((InformationObject, false)),
      2,
      typesOfMetadataFilesInRepo = List(ContentObject),
      fileContentToWriteToEachFileInRepo = List(fileContent),
      metadataElemsPreservicaResponse = Seq(
        <Metadata><Ref/><Entity/><Content><differentThing xmlns="http://www.mockSchema.com/test/v42"></differentThing></Content></Metadata>
      ),
      bitstreamInfo1Responses = List(
        BitStreamInfo(
          "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          1,
          "",
          List(Fixity("SHA256", checksum)),
          1,
          Original,
          None,
          Some(ioId)
        )
      )
    )
    val repo = utils.repo

    runCustodialCopy(utils.sqsClient, utils.config, utils.processor)

    utils.latestObjectVersion(repo, ioId) must equal(2)

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
        <XIP xmlns="http://preservica.com/XIP/v7.7">
          <ContentObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></ContentObject>
          <Generation original="true" active="true"><ContentObject>someContent</ContentObject></Generation>
          <Bitstream><Filename>90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt</Filename><FileSize>1</FileSize><Fixities><Fixity><FixityAlgorithmRef>SHA256</FixityAlgorithmRef><FixityValue>532eaabd9574880dbf76b9b8cc00832c20a6ec113d682299550d7a6e0f345e25</FixityValue></Fixity></Fixities></Bitstream>
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>
      )
    )
  }

  "runCustodialCopy" should "throw an error if the OCFL repository returns an unexpected error" in {
    val utils = new MainTestUtils(objectVersion = 0)
    val ioId = utils.ioId

    val repo = mock[MutableOcflRepository]
    when(repo.getObject(any[ObjectVersionId])).thenThrow(new RuntimeException("Unexpected Exception"))
    val semaphore: Semaphore[IO] = Semaphore[IO](1).unsafeRunSync()
    val ocflService = new OcflService(repo, semaphore)
    val processor =
      new Processor(
        utils.config,
        utils.sqsClient,
        ocflService,
        utils.preservicaClient,
        utils.xmlValidator,
        utils.snsClient
      )

    val ex: Throwable = getError(utils.sqsClient, utils.config, processor)

    ex.getMessage must equal(
      s"'getObject' returned an unexpected error 'java.lang.RuntimeException: Unexpected Exception' when called with object id $ioId"
    )
  }

  "runCustodialCopy" should "return more than 10 messages if there are more messages available" in {
    val utils = new MainTestUtils(objectVersion = 0)
    val sqsClient = utils.sqsClient
    val groupId = UUID.randomUUID.toString
    def messages = (1 to 10).map(i => MessageResponse("", Option(groupId), SoReceivedSnsMessage(UUID.randomUUID, false))).toList
    reset(sqsClient)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))
    when(sqsClient.receiveMessages(any[String], any[Int])(using any[Decoder[SoReceivedSnsMessage]]))
      .thenReturn(IO(messages))
      .thenReturn(IO(messages))
      .thenReturn(IO(Nil))

    val results = runCustodialCopy(sqsClient, utils.config, utils.processor)

    results.size must equal(20)
  }

  "runCustodialCopy" should "return 50 messages if there are more than 50 messages available" in {
    val utils = new MainTestUtils(objectVersion = 0)
    val sqsClient = utils.sqsClient
    val groupId = UUID.randomUUID.toString

    def messages = (1 to 25).map(i => MessageResponse("", Option(groupId), SoReceivedSnsMessage(UUID.randomUUID, false))).toList

    reset(sqsClient)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))
    when(sqsClient.receiveMessages(any[String], any[Int])(using any[Decoder[SoReceivedSnsMessage]]))
      .thenReturn(IO(messages))
      .thenReturn(IO(messages))
      .thenReturn(IO(messages))
      .thenReturn(IO(Nil))

    val results = runCustodialCopy(sqsClient, utils.config, utils.processor)

    results.size must equal(50)
  }

  "runCustodialCopy" should "return 52 messages if the second call takes the total messages over 50" in {
    val utils = new MainTestUtils(objectVersion = 0)
    val sqsClient = utils.sqsClient
    val groupId = UUID.randomUUID.toString

    def messages = (1 to 26).map(i => MessageResponse("", Option(groupId), SoReceivedSnsMessage(UUID.randomUUID, false))).toList

    reset(sqsClient)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))
    when(sqsClient.receiveMessages(any[String], any[Int])(using any[Decoder[SoReceivedSnsMessage]]))
      .thenReturn(IO(messages))
      .thenReturn(IO(messages))
      .thenReturn(IO(messages))
      .thenReturn(IO(Nil))

    val results = runCustodialCopy(sqsClient, utils.config, utils.processor)

    results.size must equal(52)
  }

  "runCustodialCopy" should "return no messages if there is an error getting the first messages" in {
    val utils = new MainTestUtils(objectVersion = 0)
    val sqsClient = utils.sqsClient
    val groupId = UUID.randomUUID.toString

    reset(sqsClient)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))
    when(sqsClient.receiveMessages(any[String], any[Int])(using any[Decoder[SoReceivedSnsMessage]]))
      .thenReturn(IO.raiseError(new Exception("Error getting SQS messages")))

    val results = runCustodialCopy(sqsClient, utils.config, utils.processor)

    results.size must equal(0)
  }

  "runCustodialCopy" should "return 10 messages if the first call succeeds but the subsequent call fails" in {
    val utils = new MainTestUtils(objectVersion = 0)
    val sqsClient = utils.sqsClient
    val groupId = UUID.randomUUID.toString

    def messages = (1 to 10).map(i => MessageResponse("", Option(groupId), SoReceivedSnsMessage(UUID.randomUUID, false))).toList

    reset(sqsClient)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder.build))
    when(sqsClient.receiveMessages(any[String], any[Int])(using any[Decoder[SoReceivedSnsMessage]]))
      .thenReturn(IO(messages))
      .thenReturn(IO.raiseError(new Exception("Error getting SQS messages")))

    val results = runCustodialCopy(sqsClient, utils.config, utils.processor)

    results.size must equal(10)
  }
}
