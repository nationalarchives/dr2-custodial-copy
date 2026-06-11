package uk.gov.nationalarchives.custodialcopy

import cats.effect.*
import pureconfig.*
import uk.gov.nationalarchives.custodialcopy.Main.Config

trait Configuration:
  private[custodialcopy] lazy val configOrError: Config =
    ConfigSource.default.loadOrThrow[Config]

  def config: Config

object Configuration:
  given impl: Configuration = new Configuration:
    override def config: Config = configOrError

  def apply[F[_]: Async](using ev: Configuration): Configuration = ev
