package uk.gov.nationalarchives.reindexer

import cats.effect.Async
import cats.implicits.*
import doobie.Update
import doobie.implicits.*
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.util.{Put, Write}
import fs2.Chunk
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.utils.Utils.{OcflFile, given}

trait Database[F[_]]:
  def write(files: Chunk[OcflFile]): F[Int]

object Database:
  def apply[F[_]](using db: Database[F]): Database[F] = db

  given impl[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def write(files: Chunk[OcflFile]): F[Int] =
      val insertSql =
        "insert into files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation, consignmentRef) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      for
        logger <- Slf4jLogger.create[F]
        insertCount <- Update[OcflFile](insertSql).updateMany(files).transact(xa)
        _ <- logger.info(s"$insertCount rows inserted")
      yield insertCount

  }
