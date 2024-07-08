package uk.gov.nationalarchives.builder

import cats.effect.Async
import cats.implicits.*
import uk.gov.nationalarchives.utils.Utils.OcflFile
import doobie.Update
import doobie.implicits.*
import doobie.util.Put
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID

trait Database[F[_]]:
  def write(files: List[OcflFile]): F[Unit]

object Database:
  def apply[F[_]](using ev: Database[F]): Database[F] = ev

  given Put[UUID] = Put[String].contramap(_.toString)

  given impl[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def write(files: List[OcflFile]): F[Unit] = {
      val sql = "insert into files (version, id, name, fileId, zref, path, fileName) values (?, ?, ?, ?, ?, ?, ?)"
      for {
        logger <- Slf4jLogger.create[F]
        updateCount <- Update[OcflFile](sql).updateMany(files).transact(xa)
        _ <- logger.info(s"Created $updateCount rows")
      } yield ()
    }
  }
