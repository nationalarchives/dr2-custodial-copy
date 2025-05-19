package uk.gov.nationalarchives.confirmer

import io.ocfl.api.OcflRepository
import uk.gov.nationalarchives.confirmer.Main.Config
import uk.gov.nationalarchives.utils.Utils.createOcflRepository

import java.util.UUID

trait Ocfl(val config: Config):
  private[confirmer] val repo: OcflRepository = createOcflRepository(config.ocflRepoDir, config.ocflWorkDir)

  def checkObjectExists(id: UUID): Boolean

object Ocfl:

  def apply(config: Config): Ocfl = new Ocfl(config):
    override def checkObjectExists(id: UUID): Boolean =
      repo.containsObject(id.toString)
