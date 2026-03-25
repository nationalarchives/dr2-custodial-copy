package uk.gov.nationalarchives.custodialcopy

import cats.effect.Async
import doobie.implicits.*
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.util.{Get, Read}
import uk.gov.nationalarchives.custodialcopy.Main.Config

import java.util.UUID

trait Database[F[_]]:
  def getPathFromDri(fileId: UUID): F[List[String]]

object Database:

  def apply[F[_]: Async](config: Config): Database[F] = new Database[F] {
    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def getPathFromDri(fileId: UUID): F[List[String]] = {
      val selectSql = sql"select file_path from dri_files where file_id = ${fileId.toString}"
      selectSql.query[String].to[List].transact(xa)
    }
  }
