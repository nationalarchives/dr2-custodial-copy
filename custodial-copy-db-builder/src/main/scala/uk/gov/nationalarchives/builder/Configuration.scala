package uk.gov.nationalarchives.builder

import cats.effect.Async
import Main.Config
import pureconfig.ConfigSource

trait Configuration:
  private[reconciler] val configOrError: Config = ConfigSource.default.loadOrThrow[Config]
  def config: Config
object Configuration:
  def apply[F[_]: Async](using ev: Configuration): Configuration = ev

  given impl: Configuration = new Configuration:
    override def config: Config = configOrError
