package uk.gov.nationalarchives.builder

import cats.effect.Async
import cats.implicits.*
import doobie.Update
import doobie.implicits.*
import doobie.util.Put
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import uk.gov.nationalarchives.utils.Utils.{OcflFile, given}

import java.util.UUID

trait Database[F[_]]:
  def write(files: List[OcflFile]): F[Unit]

object Database:
  def apply[F[_]](using ev: Database[F]): Database[F] = ev

  given impl[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def write(files: List[OcflFile]): F[Unit] = {
      val deleteSql = "delete from files where id = ?"
      val insertSql =
        "insert into files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      val deleteAndInsert = for {
        _ <- Update[UUID](deleteSql).updateMany(files.map(_.id))
        _ <- Update[OcflFile](insertSql).updateMany(files)
      } yield ()
      deleteAndInsert.transact(xa)
    }
  }
