package uk.gov.nationalarchives.reindexer

import cats.effect.*
import cats.implicits.*
import fs2.{Stream, Chunk}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait ReIndexer[F[_]: Sync]:
  def reIndex()(using Database[F], Ocfl[F], Concurrent[F]): F[Unit]

object ReIndexer:

  def apply[F[_]: Sync](using ev: ReIndexer[F]): ReIndexer[F] = ev

  given logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  private def logCount[F[_]: Sync](ref: Ref[F, Int], updatedCount: Int): F[Unit] =
    ref
      .getAndUpdate(_ + updatedCount)
      .flatMap(existing => Logger[F].info(s"Written rows $existing to ${existing + updatedCount - 1} to the database"))

  given impl[F[_]: {Sync, Ocfl, Database}]: ReIndexer[F] = new ReIndexer[F]:
    override def reIndex()(using Database[F], Ocfl[F], Concurrent[F]): F[Unit] =
      val chunkSize = 10000
      val ocfl = Ocfl[F]
      for {
        ref <- Ref[F].of(0)
        _ <- Ocfl[F]
          .allObjectsIds()
          .parEvalMap(1000)(Ocfl[F].generateOcflObject)
          .flatMap(Stream.emits)
          .chunkN(chunkSize)
          .evalMap(Database[F].write)
          .evalTap(logCount(ref, _))
          .compile
          .count
        total <- ref.get
        _ <- logger.info(s"Processed $total rows")
      } yield ()
