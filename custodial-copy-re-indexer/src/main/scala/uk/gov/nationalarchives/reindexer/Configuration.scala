package uk.gov.nationalarchives.reindexer

import cats.effect.*
import pureconfig.*
import uk.gov.nationalarchives.reindexer.Configuration.Config

trait Configuration:
  private[reindexer] lazy val configOrError: Config =
    ConfigSource.default.loadOrThrow[Config]

  def config: Config
object Configuration:

  def apply[F[_]: Async](using ev: Configuration): Configuration = ev

  given impl: Configuration = new Configuration:
    override def config: Config = configOrError

  case class Config(databasePath: String, ocflRepoDir: String, ocflWorkDir: String, maxConcurrency: Int) derives ConfigReader
