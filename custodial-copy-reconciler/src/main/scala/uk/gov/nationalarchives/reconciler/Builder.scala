package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original

import java.time.Instant
import java.util.UUID

trait Builder[F[_]]:
  def run(
      entityIds: Seq[UUID]
  ): F[List[CoRow]]

object Builder:
  def apply[F[_]: Async](client: EntityClient[F, Fs2Streams[F]]): Builder[F] =
    (entityIds: Seq[UUID]) => {
      entityIds.toList.flatTraverse { entityId =>
        val now = Instant.now.toEpochMilli
        client.getBitstreamInfo(entityId).map { bitstreamInfoForCo =>
          bitstreamInfoForCo.collect { // We're only concerned with original COs
            case bitstreamInfo if bitstreamInfo.generationType == Original && bitstreamInfo.generationVersion == 1 =>
              val potentialSha256 = bitstreamInfo.fixities.collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha256" => fixity.value }
              CoRow(entityId, bitstreamInfo.parentRef, potentialSha256)
          }.toList
        }
      }
    }
