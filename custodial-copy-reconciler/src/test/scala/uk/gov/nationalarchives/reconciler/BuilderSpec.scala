package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import io.ocfl.api.model.{DigestAlgorithm, OcflObjectVersionFile}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.mockito.MockitoSugar.mock
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.reconciler.OcflService
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.{Derived, Original}
import uk.gov.nationalarchives.reconciler.Builder
import uk.gov.nationalarchives.reconciler.Main.Config

import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters.*

class BuilderSpec extends AnyFlatSpec:

  "Builder run" should "return OcflCoRows, given an InformationObjectRef" in {
    val config = Config("", "", 5, Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString)
    val ref = UUID.randomUUID
    val parentRef = UUID.randomUUID
    val co1Ref = UUID.randomUUID
    val co2Ref = UUID.randomUUID
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val ocflService = mock[OcflService]
    val co1File = mock[OcflObjectVersionFile]
    val co2File = mock[OcflObjectVersionFile]
    when(ocflService.getAllObjectFiles(ArgumentMatchers.eq(ref))).thenReturn(IO.pure(List(co1File, co2File)))
    when(co1File.getStorageRelativePath).thenReturn(s"$ref/Preservation_1/$co1Ref/original/g1/$co1Ref.testExt")
    when(co1File.getFixity).thenReturn(Map(DigestAlgorithm.fromOcflName("sha256") -> "co1FileFixity").asJava)

    when(co2File.getStorageRelativePath).thenReturn(s"$ref/Access_1/$co2Ref/original/g1/$co2Ref.testExt")
    when(co2File.getFixity).thenReturn(Map(DigestAlgorithm.fromOcflName("sha1") -> "co2FileFixity").asJava)
    when(preservicaClient.getBitstreamInfo(any[UUID])).thenReturn(IO.pure(Nil))

    val entityChunks: Chunk[InformationObjectRef] = Chunk(InformationObjectRef(ref, parentRef))

    val ocflCoRows = Builder[IO].run(preservicaClient, ocflService, entityChunks).unsafeRunSync()
    ocflCoRows should equal(
      Chunk(
        CoRows(
          List(
            OcflCoRow(co1Ref, ref, "Preservation_1", "Original", Some("co1FileFixity"), None, None),
            OcflCoRow(co2Ref, ref, "Access_1", "Original", None, Some("co2FileFixity"), None)
          ),
          Nil
        )
      )
    )
  }

  "Builder run" should ", given ContentObjectRefs, return only the PreservicaCoRows for COs that have the generation type of 'Original' and generation version of '1'" in {
    val config = Config("", "", 5, Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString)
    val parentRef = UUID.randomUUID
    val co1Ref = UUID.randomUUID
    val co2Ref = UUID.randomUUID
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val ocflService = mock[OcflService]
    val bitStreamInfo1 = BitStreamInfo(s"$co1Ref.testExt", 1, "", List(Fixity("sha256", "co1FileFixity")), 1, Original, None, Some(parentRef))
    val bitStreamInfo2 = BitStreamInfo(s"$co1Ref.testExt2", 1, "", List(Fixity("sha256", "co1FileFixity2")), 2, Derived, None, Some(parentRef))
    val bitStreamInfo3 = BitStreamInfo(s"$co2Ref.testExt2", 1, "", List(Fixity("sha256", "co2FileFixity")), 2, Original, None, Some(parentRef))

    val bitStreamInfoList1 = Seq(bitStreamInfo1, bitStreamInfo2)
    val bitStreamInfoList2 = Seq(bitStreamInfo3)
    when(preservicaClient.getBitstreamInfo(ArgumentMatchers.eq(co1Ref))).thenReturn(IO(bitStreamInfoList1))
    when(preservicaClient.getBitstreamInfo(ArgumentMatchers.eq(co2Ref))).thenReturn(IO(bitStreamInfoList2))

    val entityChunks: Chunk[ContentObjectRef] = Chunk(ContentObjectRef(co1Ref, parentRef), ContentObjectRef(co2Ref, parentRef))

    val preservicaCoRows = Builder[IO].run(preservicaClient, ocflService, entityChunks).unsafeRunSync()
    preservicaCoRows should equal(
      Chunk(
        CoRows(
          Nil,
          List(
            PreservicaCoRow(co1Ref, parentRef, "Original", Some("co1FileFixity"), None, None)
          )
        )
      )
    )
  }
