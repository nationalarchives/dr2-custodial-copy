package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import fs2.Chunk
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.Entities.EntityRef
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original

import java.time.Instant


trait Builder[F[_]]:
  def run(
      client: EntityClient[F, Fs2Streams[F]],
      entityRefChunks: Chunk[EntityRef]
  ): F[Chunk[PreservicaCoRow]]

object Builder:
  def apply[F[_]: Async]: Builder[F] =
    (client: EntityClient[F, Fs2Streams[F]], entityRefChunks: Chunk[EntityRef]) => {
      val now = Instant.now.toEpochMilli
      entityRefChunks.flatTraverse {
        case EntityRef.StructuralObjectRef(soRef, potentialParentRef) =>
          Async[F].pure(Chunk(PreservicaCoRow(soRef, potentialParentRef, StructuralObject, None, now)))
        case EntityRef.InformationObjectRef(ioRef, parentRef) =>
          Async[F].pure(Chunk(PreservicaCoRow(ioRef, Option(parentRef), InformationObject, None, now)))
        case EntityRef.ContentObjectRef(ref, parentRef) =>
          client.getBitstreamInfo(ref).map { bitstreamInfoForCo =>
            Chunk.iterator(bitstreamInfoForCo.iterator).collect { // We're only concerned with original COs
              case bitstreamInfo if bitstreamInfo.generationType == Original && bitstreamInfo.generationVersion == 1 =>
                val potentialSha256 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha256" => fixity.value }
                PreservicaCoRow(ref, Option(parentRef), ContentObject, potentialSha256, now)
            }
          }
        case _ => Async[F].pure(Chunk.empty)
      }
    }
