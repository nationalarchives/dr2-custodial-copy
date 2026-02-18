package uk.gov.nationalarchives.reindexer

import cats.effect.*

object Main extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    ReIndexer[IO].reIndex().map(_ => ExitCode.Success)
