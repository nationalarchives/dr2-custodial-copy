package uk.gov.nationalarchives.reconciler

import cats.effect.{Async, IO}
import cats.implicits.*
import fs2.Chunk
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.reconciler.OcflService
import uk.gov.nationalarchives.dp.client.Entities.EntityRef
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.dp.client.{Entities, EntityClient}

import java.util.UUID
import scala.jdk.CollectionConverters.*

trait Builder[F[_]]:
  def run(
      client: EntityClient[F, Fs2Streams[F]],
      ocflService: OcflService,
      entityRefChunks: Chunk[InformationObjectRef | ContentObjectRef]
  ): F[Chunk[CoRows]]

object Builder:
  def apply[F[_]: Async]: Builder[IO] =
    (client: EntityClient[IO, Fs2Streams[IO]], ocflService: OcflService, entityRefChunks: Chunk[InformationObjectRef | ContentObjectRef]) => {
      val (ioRefChunks: Chunk[InformationObjectRef], coRefChunks: Chunk[ContentObjectRef]) =
        entityRefChunks.partitionEither {
          case informationObjectRef: InformationObjectRef => Left(informationObjectRef)
          case contentObjectRef: ContentObjectRef         => Right(contentObjectRef)
        }

      def isCoFile(storageRelativePath: String) = storageRelativePath.contains("/original/")

      val ocflCoRetrieval =
        for {
          ocflCoRows <- ioRefChunks.parTraverse { ioRef =>
            ocflService.getAllObjectFiles(ioRef.ref).map {
              _.collect {
                case coFile if isCoFile(coFile.getStorageRelativePath) =>
                  val pathAsList = coFile.getStorageRelativePath.split("/")
                  val pathStartingFromRepType = pathAsList.dropWhile(pathPart => !pathPart.startsWith("Preservation_") && !pathPart.startsWith("Access_"))
                  val repType = pathStartingFromRepType.head
                  val coRef = UUID.fromString(pathStartingFromRepType(1))
                  val genType = pathStartingFromRepType(2).capitalize

                  val fixities = coFile.getFixity.asScala.toMap.map { case (digestAlgo, value) => (digestAlgo.getOcflName, value) }
                  val potentialSha256 = fixities.get("sha256")
                  val potentialSha1 = fixities.get("sha1")
                  val potentialMd5 = fixities.get("md5")

                  OcflCoRow(coRef, ioRef.ref, repType, genType, potentialSha256, potentialSha1, potentialMd5)
              }
            }
          }
        } yield ocflCoRows
      val preservicaCoRetrieval =
        for {
          chunkOfPreservicaCoRows <- coRefChunks.traverse { coRef =>
            client.getBitstreamInfo(coRef.ref).map { bitstreamInfoForCo =>
              bitstreamInfoForCo.toList.collect { // We're only concerned with original COs
                case bitstreamInfo if bitstreamInfo.generationType == Original && bitstreamInfo.generationVersion == 1 =>
                  val potentialSha256 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha256" => fixity.value }
                  val potentialSha1 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha1" => fixity.value }
                  val potentialMd5 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "md5" => fixity.value }

                  PreservicaCoRow(coRef.ref, bitstreamInfo.parentRef.get, bitstreamInfo.generationType.toString, potentialSha256, potentialSha1, potentialMd5)
              }
            }
          }
        } yield chunkOfPreservicaCoRows
      ocflCoRetrieval.parProduct(preservicaCoRetrieval).map { (listOfOcflCoRowsChunk, listOfPreservicaCoRowsChunk) =>
        Chunk(
          CoRows(listOfOcflCoRowsChunk.head.getOrElse(Nil), listOfPreservicaCoRowsChunk.head.getOrElse(Nil))
        )
      }
    }

case class CoRows(listOfOcflCoRows: List[OcflCoRow], listOfPreservicaCoRows: List[PreservicaCoRow])
