package uk.gov.nationalarchives.utils

import cats.effect.IO
import doobie.Transactor
import doobie.implicits.*
import doobie.util.transactor.Transactor.Aux
import cats.effect.unsafe.implicits.global
import uk.gov.nationalarchives.utils.Utils.OcflFile

import java.util.UUID

object TestUtils:
  val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
    driver = "org.sqlite.JDBC",
    url = "jdbc:sqlite:test-database",
    logHandler = None
  )

  def createTable(): Int =
    sql"CREATE TABLE IF NOT EXISTS files(version int, id text, name text, fileId text, zref text, path text, fileName text);".update.run
      .transact(xa)
      .unsafeRunSync()

  def createFile(fileId: String = UUID.randomUUID.toString, zref: String = "zref"): IO[OcflFile] = {
    val id = UUID.randomUUID.toString
    sql"""INSERT INTO files (version, id, name, fileId, zref, path, fileName)
                 VALUES (1, $id, 'name', $fileId, $zref, 'path', 'fileName')""".update.run
      .transact(xa)
      .map(_ => ocflFile(id, fileId, zref))
  }

  def readFiles(id: String): IO[List[OcflFile]] = {
    sql"SELECT * FROM files where id = $id"
      .query[OcflFile]
      .to[List]
      .transact(xa)
  }

  def ocflFile(id: String, fileId: String, zref: String = "zref"): OcflFile =
    OcflFile(1, id, "name", fileId, zref, "path", "fileName")
