package uk.gov.nationalarchives.custodialcopy

import cats.effect.Async
import org.typelevel.doobie.Update
import org.typelevel.doobie.implicits.*
import org.typelevel.doobie.util.log.LogHandler
import org.typelevel.doobie.util.transactor.Transactor
import org.typelevel.doobie.util.transactor.Transactor.Aux
import org.typelevel.doobie.util.{Get, Read}

import java.time.LocalDateTime

trait Database[F[_]]:
  def getPathFromDri(fileId: String): F[Option[String]]

  def setAsDownloaded(fileIds: List[String]): F[Int]

object Database:

  def apply[F[_]: Async](icDbPath: String): Database[F] = new Database[F] {
    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:$icDbPath",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def getPathFromDri(fileId: String): F[Option[String]] = {
      val selectSql = sql"select file_path from dri_files where file_id = $fileId"
      selectSql.query[String].option.transact(xa)
    }

    override def setAsDownloaded(fileIds: List[String]): F[Int] =
      val currentDate = LocalDateTime.now.toString
      val updates = fileIds.map(id => currentDate -> id)
      val updateSql = "update dri_files set downloaded = true, downloaded_at = ? where file_id = ?"
      Update[(String, String)](updateSql).updateMany(updates).transact(xa)

  }
