package uk.gov.nationalarchives.builder

import cats.effect.IO
import uk.gov.nationalarchives.builder.Main.Config
import uk.gov.nationalarchives.builder.utils.TestUtils.*
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{okJson, post, urlEqualTo}
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import io.ocfl.api.model.DigestAlgorithm
import io.ocfl.core.util.DigestUtil
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.utils.TestUtils.{createTable, readFiles}
import cats.effect.unsafe.implicits.global
import org.scalatest.matchers.should.Matchers.*
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.xml.Elem

class MainSpec extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeAll(): Unit = createTable()

  override def beforeEach(): Unit = sqsServer.resetAll()

  def body(id: UUID): String = {
    val md5 = DigestUtil.computeDigestHex(DigestAlgorithm.md5, s"{\"ioRef\":\"$id\"}")
    s"""{"Messages": [{"Body": "{\\"ioRef\\":\\"$id\\"}","MD5OfBody": "$md5","ReceiptHandle": "A"}]}"""
  }

  val sqsServer = new WireMockServer(9001)

  sqsServer.start()

  private lazy val httpClient: SdkAsyncHttpClient = NettyNioAsyncHttpClient.builder().build()
  private val sqsClient: SqsAsyncClient = SqsAsyncClient.builder
    .region(Region.EU_WEST_2)
    .endpointOverride(URI.create("http://localhost:9001"))
    .httpClient(httpClient)
    .build()
  val daSQSClient = new DASQSClient[IO](sqsClient)

  "runBuilder" should "write the correct items to the database for multiple content objects" in {
    val id = UUID.randomUUID
    val testConfig = initialiseRepo(id)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = testConfig

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id.toString).unsafeRunSync()
    val firstFile = files.minBy(_.fileId)
    val secondFile = files.maxBy(_.fileId)

    firstFile.id should equal(id.toString)
    firstFile.name should equal("Title")
    firstFile.fileId should equal("Reference")
    firstFile.zref should equal("Zref")
    firstFile.fileName should equal("Content Title")

    secondFile.id should equal(id.toString)
    secondFile.name should equal("Title")
    secondFile.fileId should equal("Reference2")
    secondFile.zref should equal("Zref")
    secondFile.fileName should equal("Content Title2")
  }

  "runBuilder" should "write the correct items to the database for a single content object" in {
    val id = UUID.randomUUID
    val testConfig = initialiseRepo(id, coMetadataContent = completeCoMetadataContentElements.head :: Nil)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = testConfig

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id.toString).unsafeRunSync()
    files.length should equal(1)
    val firstFile = files.head
    firstFile.id should equal(id.toString)
    firstFile.name should equal("Title")
    firstFile.fileId should equal("Reference")
    firstFile.zref should equal("Zref")
    firstFile.fileName should equal("Content Title")
  }

  "runBuilder" should "copy the content file to the new location" in {
    val id = UUID.randomUUID
    val testConfig = initialiseRepo(id, coMetadataContent = completeCoMetadataContentElements.head :: Nil)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = testConfig

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id.toString).unsafeRunSync()
    files.length should equal(1)
    val firstFile = files.head
    Files.readString(Paths.get(firstFile.path)) should equal("test")
  }

  "runBuilder" should "return empty string for missing metadata values" in {
    val id = UUID.randomUUID
    val ioMetadata =
      <Metadata>
        <Identifiers/>
        <InformationObject/>
      </Metadata>

    val coMetadata =
      <Metadata>
        <ContentObject/>
      </Metadata>

    val testConfig = initialiseRepo(id, ioMetadata, coMetadata :: Nil)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = testConfig

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id.toString).unsafeRunSync()

    files.size should equal(1)

    val file = files.head
    file.id should equal(id.toString)
    file.name.isBlank should equal(true)
    file.fileId.isBlank should equal(true)
    file.zref.isBlank should equal(true)
    file.fileName.isBlank should equal(true)
  }

  "runBuilder" should "call the correct SQS endpoints" in {
    val id = UUID.randomUUID
    initialiseSqs(id)
    val testConfig = initialiseRepo(id)

    given config: Configuration = new Configuration:
      override def config: Config = testConfig

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val serveEvents = sqsServer.getAllServeEvents.asScala.toList

    serveEvents.last.getRequest.getBodyAsString should equal("""{"QueueUrl":"http://localhost:9001","MaxNumberOfMessages":10}""")
    serveEvents.head.getRequest.getBodyAsString should equal("""{"QueueUrl":"http://localhost:9001","ReceiptHandle":"A"}""")
  }

  def initialiseSqs(id: UUID): StubMapping = {
    val messageResponse = body(id)
    sqsServer.stubFor(post(urlEqualTo("/")).willReturn(okJson(messageResponse)))
  }

}
