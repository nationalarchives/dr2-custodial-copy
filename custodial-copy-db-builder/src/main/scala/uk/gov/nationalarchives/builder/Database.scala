package uk.gov.nationalarchives.builder

import cats.effect.Async
import cats.implicits.*
import doobie.Update
import doobie.free.connection.ConnectionIO
import doobie.implicits.*
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.utils.Utils.{OcflFile, given}

import java.util.UUID

trait Database[F[_]]:
  def write(files: List[OcflFile]): F[Unit]

object Database:
  def apply[F[_]](using db: Database[F]): Database[F] = db

  given impl[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def write(files: List[OcflFile]): F[Unit] = {
      val deleteSql = "delete from files where id = ?"
      val insertSql =
        "insert into files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation, consignmentRef) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      val deleteAndInsert: ConnectionIO[(Int, Int)] = for {
        deleteCount <- Update[UUID](deleteSql).updateMany(files.map(_.id))
        updateCount <- Update[OcflFile](insertSql).updateMany(files)
      } yield (updateCount, deleteCount)
      for {
        logger <- Slf4jLogger.create[F]
        _ <- logger.info(s"CCDBW: Writing files to database with consignmentRefs: ${files.map(_.consignmentRef).mkString(", ")}")
        (updateCount, deleteCount) <- deleteAndInsert.transact(xa)
        _ <- logger.info(s"$updateCount rows updated. $deleteCount rows deleted")
      } yield ()
    }
  }
