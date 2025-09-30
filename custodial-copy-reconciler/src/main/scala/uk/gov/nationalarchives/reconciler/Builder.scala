package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.EntityClient

import java.util.UUID

trait Builder[F[_]]:
  def run(
      entityIds: Seq[UUID]
  ): F[List[CoRow]]

object Builder:
  def apply[F[_]: Async](client: EntityClient[F, Fs2Streams[F]]): Builder[F] =
    (entityIds: Seq[UUID]) =>
      entityIds.toList.flatTraverse { entityId =>
        client.getBitstreamInfo(entityId).map { bitstreamInfoForCo =>
          bitstreamInfoForCo
            .map { bitstreamInfo =>
              bitstreamInfo.fixities
                .collectFirst { case fixity if fixity.algorithm.toLowerCase == "sha256" => CoRow(entityId, bitstreamInfo.parentRef, fixity.value) }
            }
            .toList
            .flatten
        }
      }
