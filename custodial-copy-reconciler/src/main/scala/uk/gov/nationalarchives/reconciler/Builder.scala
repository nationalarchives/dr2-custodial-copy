package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import fs2.Chunk
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.Entities.EntityRef
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.reconciler.OcflService

import java.util.UUID
import scala.jdk.CollectionConverters.*

trait Builder[F[_]]:
  def run(
      client: EntityClient[F, Fs2Streams[F]],
      ocflService: OcflService[F],
      entityRefChunks: Chunk[EntityRef]
  ): F[Chunk[CoRow]]

object Builder:
  def apply[F[_]: Async]: Builder[F] =
    (client: EntityClient[F, Fs2Streams[F]], ocflService: OcflService[F], entityRefChunks: Chunk[EntityRef]) => {
      def isOriginalAndPreservation(storageRelativePath: String) =
        storageRelativePath.contains("/original/") && storageRelativePath.contains("/Preservation_")

      entityRefChunks.flatTraverse {
        case EntityRef.InformationObjectRef(ioRef, parentRef) =>
          ocflService.getAllObjectFiles(ioRef).map { files =>
            Chunk.iterator(files.iterator).collect {
              case coFile if isOriginalAndPreservation(coFile.getStorageRelativePath) =>
                val pathAsList = coFile.getStorageRelativePath.split("/")
                val pathStartingFromRepType = pathAsList.dropWhile(pathPart => !pathPart.startsWith("Preservation_"))
                val coRef = UUID.fromString(pathStartingFromRepType(1))
                val fixities = coFile.getFixity.asScala.toMap.map { case (digestAlgo, value) => (digestAlgo.getOcflName, value) }
                val potentialSha256 = fixities.get("sha256")
                OcflCoRow(coRef, ioRef, potentialSha256)
            }
          }
        case EntityRef.ContentObjectRef(ref, parentRef) =>
          client.getBitstreamInfo(ref).map { bitstreamInfoForCo =>
            Chunk.iterator(bitstreamInfoForCo.iterator).collect { // We're only concerned with original COs
              case bitstreamInfo if bitstreamInfo.generationType == Original && bitstreamInfo.generationVersion == 1 =>
                val potentialSha256 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha256" => fixity.value }
                PreservicaCoRow(ref, parentRef, potentialSha256)
            }
          }
        case _ => Async[F].pure(Chunk.empty)
      }
    }
