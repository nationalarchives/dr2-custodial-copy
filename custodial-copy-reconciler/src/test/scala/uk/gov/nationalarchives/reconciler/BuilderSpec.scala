package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reconciler.Main.{Config, Message}
import uk.gov.nationalarchives.utils.Utils.OcflFile
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import fs2.Chunk
import io.ocfl.api.model.OcflObjectVersionFile
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.custodialcopy.OcflService
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef, NoEntityRef, StructuralObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.{Derived, Original}
import uk.gov.nationalarchives.reconciler.{Builder, Database}

import scala.jdk.CollectionConverters.*
import java.nio.file.Files
import java.time.Instant
import java.util.UUID

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
    when(ocflService.getAllObjectFiles(ArgumentMatchers.eq(ref))).thenReturn(IO(List(co1File, co2File)))
    when(co1File.getStorageRelativePath).thenReturn(s"$ref/Preservation_1/$co1Ref/original/g1/$co1Ref.testExt")
    when(co1File.getFixity).thenReturn(Map("sha256" -> "co1FileFixity").asJava)

    when(co2File.getStorageRelativePath).thenReturn(s"$ref/Access_1/$co2Ref/original/g1/$co2Ref.testExt")
    when(co2File.getFixity).thenReturn(Map("sha1" -> "co2FileFixity").asJava)

    val entityChunks = Chunk(InformationObjectRef(ref, parentRef))

    val ocflCoRows = Builder[IO].run(preservicaClient, ocflService, entityChunks).unsafeRunSync()
    ocflCoRows should equal(Chunk(List(
      OcflCoRow(co1Ref, ref, "Preservation_1", "Original", Some("co1FileFixity"), None, None),
      OcflCoRow(co2Ref, ref, "Access_1", "Original", None, Some("co2FileFixity"), None)
    )))
  }

  "Builder run" should ", given an ContentObjectRefs, return only the PreservicaCoRows for COs that have the generation type of 'Original' and generation version of '1'" in {
    val config = Config("", "", 5, Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString)
    val parentRef = UUID.randomUUID
    val co1Ref = UUID.randomUUID
    val co2Ref = UUID.randomUUID
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val ocflService = mock[OcflService]
    val bitStreamInfo1 = BitStreamInfo(s"$co1Ref.testExt", 1, "", List(Fixity("sha256", "co1RefFixity")), 1, Original, None, Some(parentRef))
    val bitStreamInfo2 = BitStreamInfo(s"$co1Ref.testExt2", 1, "", List(Fixity("sha256", "co1RefFixity2")), 2, Derived, None, Some(parentRef))
    val bitStreamInfo3 = BitStreamInfo(s"$co2Ref.testExt2", 1, "", List(Fixity("sha256", "co2RefFixity")), 2, Original, None, Some(parentRef))
    
    val bitStreamInfoList1 = Seq(bitStreamInfo1, bitStreamInfo2)
    val bitStreamInfoList2 = Seq(bitStreamInfo3)
    when(preservicaClient.getBitstreamInfo(ArgumentMatchers.eq(co1Ref))).thenReturn(IO(bitStreamInfoList1))
    when(preservicaClient.getBitstreamInfo(ArgumentMatchers.eq(co2Ref))).thenReturn(IO(bitStreamInfoList2))
    
    val entityChunks = Chunk(ContentObjectRef(co1Ref, parentRef), ContentObjectRef(co2Ref, parentRef))

    val preservicaCoRows = Builder[IO].run(preservicaClient, ocflService, entityChunks).unsafeRunSync()
    preservicaCoRows should equal(Chunk(List(
      PreservicaCoRow(co1Ref, ref, "Original", Some("co1FileFixity"), None, None)
    )))
  }

