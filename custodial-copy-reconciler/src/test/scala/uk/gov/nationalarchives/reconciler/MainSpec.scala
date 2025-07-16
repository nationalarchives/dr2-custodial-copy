package uk.gov.nationalarchives.reconciler

import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{equal, should}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef, NoEntityRef, StructuralObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.reconciler.Configuration
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.TestUtils.runTestReconciler
import uk.gov.nationalarchives.utils.Detail
import uk.gov.nationalarchives.utils.TestUtils.*

import java.nio.file.Files
import java.util.UUID

class MainSpec extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {

  val databaseUtils = new DatabaseUtils("test-database")
  import databaseUtils.*

  override def beforeAll(): Unit = {
    createActuallyInPsTable()
    createExpectedInPsTable()
  }

  private lazy val httpClient: SdkAsyncHttpClient = NettyNioAsyncHttpClient.builder().build()

  private def configuration(repoDir: String, workDir: String) = new Configuration:
    override def config: Config = Config("", databaseName, 5, repoDir, workDir)

  given config: Configuration = new Configuration:
    override def config: Config = Config("", databaseName, 5, "ocflRepoDir", "ocflWorkDir")

  Chunk(StructuralObjectRef(coRef, Some(ioRef)), NoEntityRef)

  "runReconciler" should "not process non-InformationObjectRefs nor non-ContentObjectRefs" in {
    val id = UUID.randomUUID()
    given Configuration = configuration(Files.createTempDirectory("repo").toString, Files.createTempDirectory("work").toString)
    val entities = List(StructuralObjectRef(coRef, Some(ioRef)), NoEntityRef)

    val eventBridgeEvents = runTestReconciler(entities, Nil)

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "not throw an error or send any messages to EventBridge if there are no files in OCFL that match the IO ref" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef, addFilesToRepo = false)
    given Configuration = configuration(repoDir, workDir)

    val eventBridgeEvents = runTestReconciler(List(InformationObjectRef(ioRef, id), ContentObjectRef(coRef, ioRef)), Nil)

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "filter out non-CO files and non-original CO files returned from OCFL" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef, addContentFilesToRepo = false)

    given Configuration = configuration(repoDir, workDir)

    val eventBridgeEvents = runTestReconciler(List(InformationObjectRef(ioRef, id), ContentObjectRef(coRef, ioRef)), Nil)

    eventBridgeEvents.size should equal(0)
  }

  "runReconciler" should "send a message to EventBridge if a CO in Preservica could not be found in CC or vice versa" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(ioRef)

    given Configuration = configuration(repoDir, workDir)
    val bitStreamInfo = BitStreamInfo(
      s"$coRef.testExt",
      1,
      "https://example.com",
      List(),
      1,
      Original,
      None,
      Some(ioRef)
    )
    val entities = List(InformationObjectRef(ioRef, id), ContentObjectRef(coRef, ioRef))
    val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

    eventBridgeEvents should equal(
      List(
        Detail(
          s"CO $coRef (parent: $ioRef) is in CC, but its checksum could not be found in Preservica\n" +
            s"CO $coRef (parent: $ioRef) is in CC, but its checksum could not be found in Preservica"
        ),
        Detail(s"CO $coRef (parent: $ioRef) is in Preservica, but its checksum could not be found in CC")
      )
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
    val entities = List(InformationObjectRef(ioRef, id), ContentObjectRef(coRef, ioRef))

    val eventBridgeEvents = runTestReconciler(entities, List(bitStreamInfo))

    eventBridgeEvents.size should equal(0)
  }
}
