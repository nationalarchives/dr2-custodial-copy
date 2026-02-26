package uk.gov.nationalarchives.reindexer

import cats.effect.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp:
  given Logger[IO] = Slf4jLogger.getLogger[IO]

  override def run(args: List[String]): IO[ExitCode] =
    ReIndexer[IO].reIndex().map(_ => ExitCode.Success)
      .handleErrorWith { err =>
        for
          logger <- Slf4jLogger.create[IO]
          _ <- logger.error(err)(err.getMessage)
        yield ExitCode.Error
      }
