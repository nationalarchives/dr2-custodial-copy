package uk.gov.nationalarchives.confirmer

import io.ocfl.api.OcflRepository
import uk.gov.nationalarchives.utils.Utils.createOcflRepository

import java.util.UUID
import scala.jdk.CollectionConverters.*

trait Ocfl(val config: CCConfig):
  private[confirmer] lazy val repo: OcflRepository = createOcflRepository(config.ocflRepoDir, config.ocflWorkDir)

  def getFilePathsForObject(id: UUID): List[String]

object Ocfl:

  def apply(config: CCConfig): Ocfl = new Ocfl(config):
    override def getFilePathsForObject(id: UUID): List[String] =
      if repo.containsObject(id.toString) then repo.describeObject(id.toString).getHeadVersion.getFiles.asScala.map(_.getPath).toList
      else List.empty
