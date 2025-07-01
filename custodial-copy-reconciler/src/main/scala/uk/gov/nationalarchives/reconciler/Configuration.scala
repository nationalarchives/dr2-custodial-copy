package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import pureconfig.ConfigSource
import uk.gov.nationalarchives.reconciler.Configuration
import uk.gov.nationalarchives.reconciler.Main.Config

trait Configuration:
  private[reconciler] val configOrError: Config = ConfigSource.default.loadOrThrow[Config]
  def config: Config
object Configuration:
  def apply[F[_] : Async](using ev: Configuration): Configuration = ev

  given impl: Configuration = new Configuration:
    override def config: Config = configOrError
