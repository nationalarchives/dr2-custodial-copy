package uk.gov.nationalarchives.reconciler

import cats.effect.std.Semaphore
import cats.effect.{Async, ExitCode, IO, IOApp}
import cats.implicits.*
import fs2.{Chunk, Stream}
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigReader, ConfigSource}
import scala.jdk.CollectionConverters.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.{DASQSClient, custodialcopy}
import uk.gov.nationalarchives.custodialcopy.OcflService
import uk.gov.nationalarchives.dp.client.Entities.EntityRef
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef, StructuralObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.{Access, Preservation}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.dp.client.{Entities, EntityClient}
import uk.gov.nationalarchives.reconciler.Configuration.impl
import uk.gov.nationalarchives.reconciler.Main.Message
import uk.gov.nationalarchives.reconciler.{Configuration, Database}

import java.util.UUID

trait Builder[IO[_]]:
  def run(client: EntityClient[IO, Fs2Streams[IO]], entityRefChunks: Chunk[EntityRef]): IO[Unit]

object Builder:
  def apply[IO[_]: {Async, Database}]: Builder[IO] =
    new Builder:
      override def run(client: EntityClient[IO, Fs2Streams[IO]], entityRefChunks: Chunk[EntityRef]): IO[Unit] =
        for {
          chunksOfCoRows <- getCoRows(client, entityRefChunks)
          _ <- chunksOfCoRows.traverse {
            case preservicaCoRows: List[PreservicaCoRow] => Database[IO].writeToActuallyInPsTable(preservicaCoRows)
            case ocflCoRows: List[OcflCoRow] => Database[IO].writeToExpectedInPsTable(ocflCoRows)
          }
        } yield ()


private def getCoRows(client: EntityClient[IO, Fs2Streams[IO]], entityRefChunks: Chunk[EntityRef]): IO[Chunk[List[CoRow]]] = {
  val (ioRefChunks, coRefChunks) =
    entityRefChunks.partitionEither {
      case informationObjectRef: InformationObjectRef => Left(informationObjectRef)
      case contentObjectRef: ContentObjectRef => Right(contentObjectRef)
    }
  val ioAndCoRefChunks = Chunk(ioRefChunks, coRefChunks)
  ioAndCoRefChunks.parFlatTraverse {
    case informationObjectRefs: Chunk[InformationObjectRef] =>
      for {
        config <- ConfigSource.default.loadF[IO, custodialcopy.Main.Config]()
        semaphore <- Semaphore[IO](1)
        service <- OcflService(config, semaphore)
        ocflCoRows <- informationObjectRefs.parTraverse { ioRef =>
          service.getAllObjectFiles(ioRef.ref).map {
            _.map { coFile =>
              val pathAsList = coFile.getStorageRelativePath.split("/")
              val pathStartingFromRepType = pathAsList.dropWhile(pathPart => !pathPart.startsWith("Preservation_") && !pathPart.startsWith("Access_"))
              val repType = pathStartingFromRepType.head
              val coRef = UUID.fromString(pathStartingFromRepType(1))

              val fixities = coFile.getFixity.asScala.toMap.map { case (digestAlgo, value) => (digestAlgo.getOcflName, value) }
              val potentialSha256 = fixities.get("sha256")
              val potentialSha1 = fixities.get("sha1")
              val potentialMd5 = fixities.get("md5")
              OcflCoRow(coRef, ioRef.ref, repType, potentialSha256, potentialSha1, potentialMd5)
            }
          }
        }
      } yield ocflCoRows
    case contentObjectRefs: Chunk[ContentObjectRef] =>
      for {
        chunkOfPreservicaCoRows <- contentObjectRefs.traverse { coRef =>
          client.getBitstreamInfo(coRef.ref).map { bitstreamInfoForCo =>
            bitstreamInfoForCo.toList.collect { // We're only concerned with the original CO
              case bitstreamInfo if bitstreamInfo.generationType == Original && bitstreamInfo.generationVersion == 1 =>
                val potentialSha256 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha256" => fixity.value }
                val potentialSha1 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha1" => fixity.value }
                val potentialMd5 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "md5" => fixity.value }
                PreservicaCoRow(coRef.ref, bitstreamInfo.parentRef.get, potentialSha256, potentialSha1, potentialMd5)
            }
          }
        }
        //q <- chunkOfPreservicaCoRows.traverse { preservicaCoRows => Database[IO].writeToActuallyInPsTable(preservicaCoRows) }
      } yield chunkOfPreservicaCoRows
  }
  //        val streamOfCoRows = coRowChunks.map(coRowChunk => Stream.chunk(coRowChunk.flatten))
  //        streamOfCoRows
}

