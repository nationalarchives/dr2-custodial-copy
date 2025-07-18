package uk.gov.nationalarchives.utils

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import doobie.implicits.*
import doobie.util.transactor.Transactor.Aux
import doobie.{Fragment, Transactor}
import io.circe.{Decoder, Encoder}
import io.ocfl.api.io.FixityCheckInputStream
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId, VersionInfo}
import io.ocfl.api.{OcflFileRetriever, OcflObjectUpdater, OcflOption}
import software.amazon.awssdk.services.sqs.model.{DeleteMessageResponse, GetQueueAttributesResponse, QueueAttributeName, SendMessageResponse}
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.utils.Utils.{OcflFile, createOcflRepository, given}

import java.io.{ByteArrayInputStream, InputStream}
import java.lang
import java.nio.file.Files
import java.time.Instant
import java.time.temporal.ChronoField
import java.util.UUID
import scala.jdk.FunctionConverters.*
import scala.xml.Elem

object TestUtils:

  class DatabaseUtils(val databaseName: String):
    val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:$databaseName",
      logHandler = None
    )

    def createReIndexerTable(): Int =
      sql"CREATE TABLE IF NOT EXISTS files(version int, id text, name text, fileId text, zref text, path text, fileName text, ingestDateTime numeric, sourceId text, citation text, consignmnetRef text);".update.run
        .transact(xa)
        .unsafeRunSync()

    def createFilesTable(): Unit = {
      val transaction = for {
        _ <- sql"DROP TABLE IF EXISTS files".update.run
        _ <-
          sql"CREATE TABLE files(version int, id text, name text, fileId text, zref text, path text, fileName text, ingestDateTime numeric, sourceId text, citation text, consignmentRef text);".update.run
      } yield ()
      transaction.transact(xa).unsafeRunSync()
    }

    def createOcflCOsTable(): Unit = {
      val transaction = for {
        _ <- sql"DROP TABLE IF EXISTS OcflCOs".update.run
        _ <-
          sql"CREATE TABLE OcflCOs(coRef text, ioRef text, sha256Checksum text);".update.run
      } yield ()
      transaction.transact(xa).unsafeRunSync()
    }

    def createPreservicaCOsTable(): Unit = {
      val transaction = for {
        _ <- sql"DROP TABLE IF EXISTS PreservicaCOs".update.run
        _ <- sql"CREATE TABLE PreservicaCOs(coRef text, ioRef text, sha256Checksum text);".update.run
      } yield ()
      transaction.transact(xa).unsafeRunSync()
    }

    def addColumn(columnName: String): Unit =
      (fr"ALTER TABLE files ADD COLUMN" ++ Fragment.const(columnName)).update.run.transact(xa).unsafeRunSync()

    def getColumn(id: UUID, columnName: String): String =
      (fr"select" ++ Fragment.const(columnName) ++ fr"from files where id = $id").query[String].unique.transact(xa).unsafeRunSync()

    def createFile(fileId: UUID = UUID.randomUUID, zref: String = "zref", id: UUID = UUID.randomUUID): IO[OcflFile] = {
      sql"""INSERT INTO files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation, consignmentRef)
                   VALUES (1, ${id.toString}, 'name', ${fileId.toString}, $zref, 'path', 'fileName', '2024-07-03T11:39:15.372Z', 'sourceId', 'citation', 'consignmentRef')""".update.run
        .transact(xa)
        .map(_ => ocflFile(id, fileId, zref))
    }

    def createFile(
        id: UUID,
        zref: Option[String],
        sourceId: Option[String],
        citation: Option[String],
        ingestDateTime: Option[Instant],
        consignmentRef: Option[String]
    ): IO[OcflFile] = {
      val fileId = UUID.randomUUID
      sql"""INSERT INTO files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation, consignmentRef)
                     VALUES (1, $id, 'name', $fileId, $zref, 'path', 'fileName', $ingestDateTime, $sourceId, $citation, $consignmentRef)""".update.run
        .transact(xa)
        .map(_ => OcflFile(1, id, "name".some, fileId, zref, "path".some, "fileName".some, ingestDateTime, sourceId, citation, consignmentRef))
    }

    def readFiles(id: UUID): IO[List[OcflFile]] = {
      sql"SELECT * FROM files where id = $id"
        .query[OcflFile]
        .to[List]
        .transact(xa)
    }

  def ocflFile(id: UUID, fileId: UUID, zref: String = "zref"): OcflFile =
    OcflFile(
      1,
      id,
      "name".some,
      fileId,
      zref.some,
      "path".some,
      "fileName".some,
      Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0).some,
      "sourceId".some,
      "citation".some,
      "consignmentRef".some
    )

  extension (uuid: UUID) def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)

  val coRef: UUID = UUID.randomUUID
  val coRefTwo: UUID = UUID.randomUUID
  val ioRef: UUID = UUID.randomUUID

  val completeIoMetadataContent: Elem = <XIP>
    <Identifier>
      <Type>BornDigitalRef</Type>
      <Value>Zref</Value>
    </Identifier>
    <Identifier>
      <Type>SourceID</Type>
      <Value>SourceID</Value>
    </Identifier>
    <Identifier>
      <Type>NeutralCitation</Type>
      <Value>Citation</Value>
    </Identifier>
    <Identifier>
      <Type>ConsignmentReference</Type>
      <Value>TDR-2025-RNDM</Value>
    </Identifier>
    <InformationObject>
      <Title>Title</Title>
      <Ref>{ioRef}</Ref>
    </InformationObject>
    <Metadata>
      <Content>
        <Source>
          <IngestDateTime>2024-07-03T11:39:15.372Z</IngestDateTime>
        </Source>
      </Content>
    </Metadata>
  </XIP>
  val completeCoMetadataContentElements: List[Elem] = List(
    <Metadata>
      <ContentObject>
        <Title>Content Title</Title>
        <Ref>{coRef}</Ref>
      </ContentObject>
    </Metadata>,
    <Metadata>
      <ContentObject>
        <Title>Content Title2</Title>
        <Ref>{coRefTwo}</Ref>
      </ContentObject>
    </Metadata>
  )

  def initialiseRepo(
      id: UUID,
      ioMetadataContent: Elem = completeIoMetadataContent,
      coMetadataContent: List[Elem] = completeCoMetadataContentElements,
      addFilesToRepo: Boolean = true,
      addContentFilesToRepo: Boolean = true,
      metadataFileSuffix: String = "_Metadata"
  ): (String, String) = {
    val repoDir = Files.createTempDirectory("repo")
    val workDir = Files.createTempDirectory("work")
    val metadataFileDirectory = Files.createTempDirectory("metadata")
    val ioMetadataFile = Files.createFile(metadataFileDirectory.resolve("IO_Metadata.xml"))

    val contentFile = Files.createTempFile("content", "file")
    Files.write(contentFile, "test".getBytes)

    Files.write(ioMetadataFile, ioMetadataContent.toString.getBytes)

    if (addFilesToRepo)
      createOcflRepository(repoDir.toString, workDir.toString).updateObject(
        id.toHeadVersion,
        new VersionInfo(),
        { (updater: OcflObjectUpdater) =>
          updater.addPath(ioMetadataFile, s"IO$metadataFileSuffix.xml", OcflOption.OVERWRITE)
          coMetadataContent.zipWithIndex.foreach { (elem, idx) =>
            val coMetadataFile = Files.createFile(metadataFileDirectory.resolve(s"$metadataFileSuffix$idx.xml"))
            val coRef = (elem \ "ContentObject" \ "Ref").head.text
            Files.write(coMetadataFile, elem.toString.getBytes)
            updater.addPath(coMetadataFile, s"Preservation_1/$coRef/CO$metadataFileSuffix.xml", OcflOption.OVERWRITE)
            if addContentFilesToRepo then updater.addPath(contentFile, s"Preservation_1/$coRef/original/g1/content.file", OcflOption.OVERWRITE)
          }
        }.asJava
      )
    (repoDir.toString, workDir.toString)
  }

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  class TestSqsClient extends DASQSClient[IO]:
    override def sendMessage[T <: Product](
        queueUrl: String
    )(message: T, potentialFifoConfiguration: Option[DASQSClient.FifoQueueConfiguration], delaySeconds: Int)(using enc: Encoder[T]): IO[SendMessageResponse] =
      notImplemented

    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[DASQSClient.MessageResponse[T]]] =
      notImplemented

    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = notImplemented

    override def getQueueAttributes(queueUrl: String, attributeNames: List[QueueAttributeName]): IO[GetQueueAttributesResponse] = notImplemented

val testOcflFileRetriever: OcflFileRetriever = new OcflFileRetriever:
  override def retrieveFile(): FixityCheckInputStream =
    new FixityCheckInputStream(new ByteArrayInputStream("".getBytes), DigestAlgorithm.fromOcflName("sha256"), "checksum")

  override def retrieveRange(startPosition: lang.Long, endPosition: lang.Long): InputStream = new ByteArrayInputStream("".getBytes)
