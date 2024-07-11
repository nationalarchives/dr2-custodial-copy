package uk.gov.nationalarchives.utils

import cats.effect.IO
import doobie.{Get, Transactor}
import doobie.implicits.*
import doobie.util.transactor.Transactor.Aux
import cats.effect.unsafe.implicits.global
import doobie.util.Put
import uk.gov.nationalarchives.utils.Utils.OcflFile
import uk.gov.nationalarchives.utils.Utils.given
import cats.syntax.all.*

import java.time.Instant
import java.time.temporal.ChronoField
import java.util.UUID

object TestUtils:
  val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
    driver = "org.sqlite.JDBC",
    url = "jdbc:sqlite:test-database",
    logHandler = None
  )

  def createTable(): Int =
    sql"CREATE TABLE IF NOT EXISTS files(version int, id text, name text, fileId text, zref text, path text, fileName text, ingestDateTime numeric, sourceId text, citation text);".update.run
      .transact(xa)
      .unsafeRunSync()

  def createFile(fileId: UUID = UUID.randomUUID, zref: String = "zref"): IO[OcflFile] = {
    val id = UUID.randomUUID
    sql"""INSERT INTO files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation)
                 VALUES (1, ${id.toString}, 'name', ${fileId.toString}, $zref, 'path', 'fileName', '2024-07-03T11:39:15.372Z', 'sourceId', 'citation')""".update.run
      .transact(xa)
      .map(_ => ocflFile(id, fileId, zref))
  }

  def createFile(id: UUID, zref: Option[String], sourceId: Option[String], citation: Option[String], ingestDateTime: Option[Instant]): IO[OcflFile] = {
    val fileId = UUID.randomUUID
    sql"""INSERT INTO files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation)
                   VALUES (1, $id, 'name', $fileId, $zref, 'path', 'fileName', $ingestDateTime, $sourceId, $citation)""".update.run
      .transact(xa)
      .map(_ => OcflFile(1, id, "name".some, fileId, zref, "path".some, "fileName".some, ingestDateTime, sourceId, citation))
  }

  def readFiles(id: UUID): IO[List[OcflFile]] = {
    sql"SELECT * FROM files where id = $id"
      .query[OcflFile]
      .to[List]
      .transact(xa)
  }

  def ocflFile(id: UUID, fileId: UUID, zref: String = "zref"): OcflFile =
    OcflFile(
      1,
      id,
      "name".some,
      fileId,
      zref.some,
      "path".some,
      "fileName".some,
      Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0).some,
      "sourceId".some,
      "citation".some
    )
