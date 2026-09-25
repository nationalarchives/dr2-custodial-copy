package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import fs2.Chunk
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.reconciler.Database.CoRow

import java.util.UUID
import fs2.Stream
import cats.effect.implicits.*

trait Builder[F[_]]:
  def run(
      entityIds: Chunk[UUID]
  ): Stream[F, CoRow]

object Builder:
  def apply[F[_]: Async](client: EntityClient[F, Fs2Streams[F]]): Builder[F] =
    (entityIds: Chunk[UUID]) =>
      Stream
        .eval(entityIds.toList.parFlatTraverse { entityId =>
          client.bitstreamForAsset(entityId).map { bitstreamInfo =>
            bitstreamInfo
              .map { bsInfo =>
                bsInfo.fixities
                  .collectFirst {
                    case fixity if fixity.algorithm.toLowerCase == "sha256" =>
                      CoRow(bsInfo.contentObjectRef, bsInfo.parentRef, fixity.value, bsInfo.generation.effectiveDate.toOffsetDateTime)
                  }
              }
              .toList
              .flatten
          }
        })
        .flatMap(Stream.emits)
