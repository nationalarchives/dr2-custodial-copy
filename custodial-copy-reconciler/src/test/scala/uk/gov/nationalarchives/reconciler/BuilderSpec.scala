package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import io.ocfl.api.model.{DigestAlgorithm, FileDetails, OcflObjectVersionFile}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.EntityClient.Generation
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.{Derived, Original}
import uk.gov.nationalarchives.reconciler.Builder
import uk.gov.nationalarchives.reconciler.Database.CoRow
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.TestUtils.{testEntityClient, testOcflService}
import uk.gov.nationalarchives.utils.testOcflFileRetriever

import java.nio.file.Files
import java.time.{OffsetDateTime, ZonedDateTime}
import java.util.UUID
import scala.jdk.CollectionConverters.*

class BuilderSpec extends AnyFlatSpec:

  "Builder run" should ", given a ContentObject, return all Preservica CoRows regardless of generation type and version" in {
    val config = Config("", "", 5, Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString, 0)
    val potentialParentRef = Some(UUID.randomUUID)
    val co1Ref = UUID.randomUUID
    val co2Ref = UUID.randomUUID
    val effectiveDate = ZonedDateTime.now
    val generation1 = Generation(effectiveDate, Original, 1)
    val generation2 = Generation(effectiveDate, Derived, 2)
    val generation3 = Generation(effectiveDate, Original, 2)
    val bitStreamInfo1 = BitStreamInfo(s"$co1Ref.testExt", 1, None, List(Fixity("sha256", "co1FileFixity")), None, potentialParentRef, generation1, co1Ref)
    val bitStreamInfo2 = BitStreamInfo(s"$co1Ref.testExt2", 1, None, List(Fixity("sha256", "co1FileFixity2")), None, potentialParentRef, generation2, co1Ref)
    val bitStreamInfo3 = BitStreamInfo(s"$co2Ref.testExt2", 1, None, List(Fixity("sha256", "co2FileFixity")), None, potentialParentRef, generation3, co2Ref)

    val bitStreamInfoList = List(bitStreamInfo1, bitStreamInfo2, bitStreamInfo3)

    val preservicaClient = testEntityClient(Map(potentialParentRef.get -> bitStreamInfoList))
    val ocflService = testOcflService(Nil)

    val entityIds: Chunk[UUID] = Chunk(potentialParentRef.get)
    val startDate = OffsetDateTime.now

    val preservicaCoRows = Builder[IO](preservicaClient, startDate).run(entityIds).compile.toList.unsafeRunSync()

    preservicaCoRows.length should equal(3)

    preservicaCoRows.contains(CoRow(co1Ref, potentialParentRef, "co1FileFixity")) should equal(true)
    preservicaCoRows.contains(CoRow(co1Ref, potentialParentRef, "co1FileFixity2")) should equal(true)
    preservicaCoRows.contains(CoRow(co2Ref, potentialParentRef, "co2FileFixity")) should equal(true)
  }

  private def createObjectVersionFile(path: String, fixityMap: Map[DigestAlgorithm, String]) =
    val fileDetails = new FileDetails()
    fileDetails.setFixity(fixityMap.asJava)
    fileDetails.setStorageRelativePath(path)
    new OcflObjectVersionFile(fileDetails, testOcflFileRetriever)
