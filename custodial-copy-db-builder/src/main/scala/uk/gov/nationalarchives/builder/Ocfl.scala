package uk.gov.nationalarchives.builder

import cats.effect.kernel.Async
import io.ocfl.api.OcflRepository
import uk.gov.nationalarchives.builder.Main.Config
import uk.gov.nationalarchives.utils.Utils
import uk.gov.nationalarchives.utils.Utils.*

import java.util.UUID

trait Ocfl[F[_]](val config: Config):
  private[builder] val repo: OcflRepository = createOcflRepository(config.ocflRepoDir, config.ocflWorkDir)

  def generateOcflObjects(id: UUID): F[List[OcflFile]]

object Ocfl:
  def apply[F[_]](using ev: Ocfl[F]): Ocfl[F] = ev

  given impl[F[_]: Async](using configuration: Configuration): Ocfl[F] = new Ocfl[F](configuration.config):
    override def generateOcflObjects(id: UUID): F[List[OcflFile]] = Async[F].blocking(Utils.generateOcflObjects(id, repo, config.ocflRepoDir))
