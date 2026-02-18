package uk.gov.nationalarchives.reconciler

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{equal, should}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.reconciler.Configuration
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.TestUtils.{DatedEntity, runTestReconciler}
import uk.gov.nationalarchives.utils.Detail
import uk.gov.nationalarchives.utils.TestUtils.*

import java.nio.file.{Files, Path}
import java.time.OffsetDateTime
import java.util.UUID

class MainSpec extends AnyFlatSpec with BeforeAndAfterEach {

  val databaseName = "test-database"
  val databaseUtils = new DatabaseUtils(databaseName)

  override def beforeEach(): Unit = {
    Files.deleteIfExists(Path.of(databaseName))
    databaseUtils.createPreservicaCOsTable()
    databaseUtils.createOcflCOsTable()
  }

  private lazy val httpClient: SdkAsyncHttpClient = NettyNioAsyncHttpClient.builder().build()

  private def configuration(repoDir: String, workDir: String) = new Configuration:
    override def config: Config = Config("", databaseName, 5, repoDir, workDir, 0)

  given config: Configuration = new Configuration:
    override def config: Config = Config("", databaseName, 5, "ocflRepoDir", "ocflWorkDir", 0)

  val bitStreamInfo = BitStreamInfo(
    s"$coRef.testExt",
    1,
    "https://example.com",
    List(Fixity("sha256", "mismatch")),
    1,
    Original,
    None,
    Some(ioRef)
  )

  def createEntity(ref: UUID, entityType: EntityType, date: OffsetDateTime) = DatedEntity(date, Entity(Option(entityType), ref, None, None, false, None, None))
  def createSO(ref: UUID, date: OffsetDateTime = OffsetDateTime.now): DatedEntity = createEntity(ref, StructuralObject, date)
  def createIO(ref: UUID, date: OffsetDateTime = OffsetDateTime.now): DatedEntity = createEntity(ref, InformationObject, date)
  def createCO(ref: UUID, date: OffsetDateTime = OffsetDateTime.now): DatedEntity = createEntity(ref, ContentObject, date)

  "runReconciler" should "not process non-InformationObjectRefs nor non-ContentObjectRefs" in {
    val id = UUID.randomUUID()
    given Configuration = configuration(Files.createTempDirectory("repo").toString, Files.createTempDirectory("work").toString)
    val entities = List(createSO(id))

    val eventBridgeEvents = runTestReconciler(entities, Nil)

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "not throw an error or send any messages to EventBridge if there are no files in OCFL that match the IO ref" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef, addFilesToRepo = false)
    given Configuration = configuration(repoDir, workDir)

    val eventBridgeEvents = runTestReconciler(List(createIO(ioRef), createCO(coRef)), Nil)

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "filter out non-CO files returned from OCFL" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef, addContentFilesToRepo = false)

    given Configuration = configuration(repoDir, workDir)

    val eventBridgeEvents = runTestReconciler(List(createIO(ioRef), createCO(coRef)), Nil)

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "send a message to EventBridge if a CO in Preservica could not be found in CC or vice versa" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef)

    given Configuration = configuration(repoDir, workDir)

    val entities = List(createCO(coRef))
    val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

    eventBridgeEvents.sortBy(_.slackMessage) should equal(
      List(
        Detail(s":alert-noflash-slow: CO $coRefTwo is in CC, but its checksum could not be found in Preservica"),
        Detail(s":alert-noflash-slow: CO $coRef is in CC, but its checksum could not be found in Preservica"),
        Detail(s":alert-noflash-slow: CO $coRef is in Preservica, but its checksum could not be found in CC")
      ).sortBy(_.slackMessage)
    )
  }

  "runReconciler" should "not send a message to EventBridge if the config is set to ignore files newer than 1 day" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef)

    given Configuration = new Configuration:
      override def config: Config = Config("", databaseName, 5, repoDir, workDir, 1)

    val entities = List(createCO(coRef))
    val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "send a message to EventBridge if a CO in Preservica could not be found in CC or vice versa, " +
    "the config is set to ignore files newer than 1 day and both Preservica entity and OCFL are older than one day " in {
      val id = UUID.randomUUID
      val (repoDir, workDir) = initialiseRepo(ioRef, createdBeforeDays = 3)

      given Configuration = new Configuration:
        override def config: Config = Config("", databaseName, 5, repoDir, workDir, 1)

      val entities = List(createCO(coRef, OffsetDateTime.now.minusDays(3)))
      val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

      eventBridgeEvents.sortBy(_.slackMessage) should equal(
        List(
          Detail(s":alert-noflash-slow: CO $coRefTwo is in CC, but its checksum could not be found in Preservica"),
          Detail(s":alert-noflash-slow: CO $coRef is in CC, but its checksum could not be found in Preservica"),
          Detail(s":alert-noflash-slow: CO $coRef is in Preservica, but its checksum could not be found in CC")
        ).sortBy(_.slackMessage)
      )
    }

  "runReconciler" should "send a message to EventBridge if the config is set to ignore files newer than 1 day and only the Preservica entity is older than one day " in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef)

    given Configuration = new Configuration:
      override def config: Config = Config("", databaseName, 5, repoDir, workDir, 1)

    val entities = List(createCO(coRef, OffsetDateTime.now.minusDays(3)))
    val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

    eventBridgeEvents.sortBy(_.slackMessage) should equal(
      List(
        Detail(s":alert-noflash-slow: CO $coRef is in Preservica, but its checksum could not be found in CC")
      ).sortBy(_.slackMessage)
    )
  }

  "runReconciler" should "send a message to EventBridge if the config is set to ignore files newer than 1 day and only the OCFL entity is older than one day " in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef, createdBeforeDays = 3)

    given Configuration = new Configuration:
      override def config: Config = Config("", databaseName, 5, repoDir, workDir, 1)

    val bitStreamInfo = BitStreamInfo(
      s"$coRef.testExt",
      1,
      "https://example.com",
      List(Fixity("sha256", "mismatch")),
      1,
      Original,
      None,
      Some(ioRef)
    )
    val entities = List(createCO(coRef))
    val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

    eventBridgeEvents.sortBy(_.slackMessage) should equal(
      List(
        Detail(s":alert-noflash-slow: CO $coRefTwo is in CC, but its checksum could not be found in Preservica"),
        Detail(s":alert-noflash-slow: CO $coRef is in CC, but its checksum could not be found in Preservica")
      ).sortBy(_.slackMessage)
    )
  }

  "runReconciler" should "not send a message to EventBridge if a CO in Preservica could be found in CC or vice versa" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef)

    val bitStreamInfo = BitStreamInfo(
      s"$coRef.testExt",
      1,
      "https://example.com",
      List(Fixity("SHA256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")),
      1,
      Original,
      None,
      Some(ioRef)
    )

    given Configuration = configuration(repoDir, workDir)
    val entities = List(createIO(ioRef), createCO(coRef))

    val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "empty both database tables before starting" in {
    val repoDir = Files.createTempDirectory("repo").toString
    val workDir = Files.createTempDirectory("work").toString

    databaseUtils.createPreservicaCORow()
    databaseUtils.createOcflCORow()

    databaseUtils.countOcflCORows() should equal(1)
    databaseUtils.countPreservicaCORows() should equal(1)

    given Configuration = configuration(repoDir, workDir)
    runTestReconciler(Nil, Nil)

    databaseUtils.countOcflCORows() should equal(0)
    databaseUtils.countPreservicaCORows() should equal(0)
  }
}
