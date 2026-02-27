package uk.gov.nationalarchives.reindexer

import cats.effect.*
import cats.implicits.*
import fs2.{Stream, Chunk}
import org.typelevel.log4cats.Logger

trait ReIndexer[F[_]: Concurrent]:
  def reIndex()(using configuration: Configuration): F[Unit]

object ReIndexer:

  def apply[F[_]: Concurrent](using ev: ReIndexer[F]): ReIndexer[F] = ev

  private def logCount[F[_]: {Logger, Concurrent}](ref: Ref[F, Int], updatedCount: Int): F[Unit] =
    ref
      .getAndUpdate(_ + updatedCount)
      .flatMap(existing => Logger[F].info(s"Written rows $existing to ${existing + updatedCount - 1} to the database"))

  given impl[F[_]: {Ocfl, Database, Concurrent, Logger}]: ReIndexer[F] = new ReIndexer[F]:
    override def reIndex()(using configuration: Configuration): F[Unit] =
      val maxConcurrency = configuration.config.maxConcurrency
      for
        ref <- Ref[F].of(0)
        count <- Ocfl[F]
          .allObjectsIds()
          .parEvalMap(maxConcurrency)(Ocfl[F].generateOcflObject)
          .flatMap(Stream.emits)
          .chunkN(maxConcurrency)
          .evalMap(Database[F].write)
          .evalTap(logCount(ref, _))
          .compile
          .count
        total <- ref.get
        _ <- Logger[F].info(s"Processed $total rows")
        _ <- Logger[F].info(s"Stream count $count")
      yield ()
