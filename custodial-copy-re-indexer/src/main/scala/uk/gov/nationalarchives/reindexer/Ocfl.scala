package uk.gov.nationalarchives.reindexer

import cats.effect.Sync
import fs2.*
import io.ocfl.api.OcflRepository
import uk.gov.nationalarchives.utils.Utils
import uk.gov.nationalarchives.utils.Utils.*

import java.util.UUID
import scala.jdk.CollectionConverters.*

trait Ocfl[F[_]: Sync]:

  def allObjectsIds(): Stream[F, UUID]

  def generateOcflObject(id: UUID): F[List[OcflFile]]

object Ocfl:
  def apply[F[_]: Sync](using ev: Ocfl[F]): Ocfl[F] = ev

  given impl[F[_]: Sync](using configuration: Configuration): Ocfl[F] = new Ocfl[F]:

    val ocflRepo: OcflRepository = createOcflRepository(configuration.config.ocflRepoDir, configuration.config.ocflWorkDir)

    override def generateOcflObject(id: UUID): F[List[OcflFile]] =
      Sync[F].blocking(Utils.generateOcflObjects(id, ocflRepo, configuration.config.ocflRepoDir))

    override def allObjectsIds(): Stream[F, UUID] =
      Stream
        .fromIterator(ocflRepo.listObjectIds().iterator().asScala, 10000)
        .map(UUID.fromString)
