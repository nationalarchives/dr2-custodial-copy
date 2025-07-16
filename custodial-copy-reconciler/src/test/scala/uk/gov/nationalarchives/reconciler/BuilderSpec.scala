package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import io.ocfl.api.model.{DigestAlgorithm, FileDetails, OcflObjectVersionFile}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.{Derived, Original}
import uk.gov.nationalarchives.reconciler.Builder
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.TestUtils.{testEntityClient, testOcflService}
import uk.gov.nationalarchives.utils.testOcflFileRetriever

import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters.*

class BuilderSpec extends AnyFlatSpec:

  "Builder run" should ", given an InformationObjectRef return only the OcflCoRows for COs that have the generation type of 'Original' and representation type 'Preservation'" in {
    val config = Config("", "", 5, Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString)
    val ref = UUID.randomUUID
    val parentRef = UUID.randomUUID
    val co1Ref = UUID.randomUUID

    val co1File =
      createObjectVersionFile(s"$ref/Preservation_1/$co1Ref/original/g1/$co1Ref.testExt", Map(DigestAlgorithm.fromOcflName("sha256") -> "co1FileFixity"))
    val co2File = createObjectVersionFile(s"$ref/Access_1/$co1Ref/original/g1/$co1Ref.testExt", Map(DigestAlgorithm.fromOcflName("sha256") -> "co2FileFixity"))
    val co3File =
      createObjectVersionFile(s"$ref/Preservation_1/$co1Ref/not-original/g1/$co1Ref.testExt", Map(DigestAlgorithm.fromOcflName("sha256") -> "co3FileFixity"))
    val co4File =
      createObjectVersionFile(s"$ref/Preservation_2/$co1Ref/original/g1/$co1Ref.testExt", Map(DigestAlgorithm.fromOcflName("sha256") -> "co4FileFixity"))

    val ocflService = testOcflService(List(co1File, co2File, co3File, co4File))
    val preservicaClient = testEntityClient(Map.empty)

    val entityChunks: Chunk[InformationObjectRef] = Chunk(InformationObjectRef(ref, parentRef))

    val ocflCoRows = Builder[IO].run(preservicaClient, ocflService, entityChunks).unsafeRunSync()
    ocflCoRows should equal(
      Chunk(CoRows(List(OcflCoRow(co1Ref, ref, Some("co1FileFixity")), OcflCoRow(co1Ref, ref, Some("co4FileFixity"))), Nil))
    )
  }

  "Builder run" should ", given ContentObjectRefs, return only the PreservicaCoRows for COs that have the generation type of 'Original' and generation version of '1'" in {
    val config = Config("", "", 5, Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString)
    val parentRef = UUID.randomUUID
    val co1Ref = UUID.randomUUID
    val co2Ref = UUID.randomUUID
    val bitStreamInfo1 = BitStreamInfo(s"$co1Ref.testExt", 1, "", List(Fixity("sha256", "co1FileFixity")), 1, Original, None, Some(parentRef))
    val bitStreamInfo2 = BitStreamInfo(s"$co1Ref.testExt2", 1, "", List(Fixity("sha256", "co1FileFixity2")), 2, Derived, None, Some(parentRef))
    val bitStreamInfo3 = BitStreamInfo(s"$co2Ref.testExt2", 1, "", List(Fixity("sha256", "co2FileFixity")), 2, Original, None, Some(parentRef))

    val bitStreamInfoList1 = List(bitStreamInfo1, bitStreamInfo2)
    val bitStreamInfoList2 = List(bitStreamInfo3)
    val preservicaClient = testEntityClient(Map(co1Ref -> bitStreamInfoList1, co2Ref -> bitStreamInfoList2))
    val ocflService = testOcflService(Nil)

    val entityChunks: Chunk[ContentObjectRef] = Chunk(ContentObjectRef(co1Ref, parentRef), ContentObjectRef(co2Ref, parentRef))

    val preservicaCoRows = Builder[IO].run(preservicaClient, ocflService, entityChunks).unsafeRunSync()
    preservicaCoRows should equal(
      Chunk(
        CoRows(
          Nil,
          List(
            PreservicaCoRow(co1Ref, parentRef, Some("co1FileFixity"))
          )
        )
      )
    )
  }

  private def createObjectVersionFile(path: String, fixityMap: Map[DigestAlgorithm, String]) =
    val fileDetails = new FileDetails()
    fileDetails.setFixity(fixityMap.asJava)
    fileDetails.setStorageRelativePath(path)
    new OcflObjectVersionFile(fileDetails, testOcflFileRetriever)
