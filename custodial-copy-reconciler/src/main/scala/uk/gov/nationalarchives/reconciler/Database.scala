package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import doobie.Update
import doobie.free.connection.ConnectionIO
import doobie.implicits.*
import doobie.util.Write
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType
import uk.gov.nationalarchives.reconciler.Database
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.utils.Utils.{OcflFile, given}

import java.util.UUID

trait Database[F[_]]:
  def writeToActuallyInPsTable(cosInPS: List[PreservicaCoRow]): F[Unit]
  def writeToExpectedInPsTable(expectedCosInPS: List[OcflCoRow]): F[Unit]

object Database:
  def apply[F[_]](using db: Database[F]): Database[F] = db

  given impl[F[_]: Async](using configuration: Configuration): Database[F] =  new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def writeToActuallyInPsTable(files: List[OcflFile]): F[Unit] = {
      val deleteSql = "delete from COsInPs where id = ?"
      val insertSql =
        "insert into COsInPs (ioRef, coRef, checksum) values (?, ?, ?)"
      val deleteAndInsert: ConnectionIO[(Int, Int)] = for {
        updateCount <- Write[OcflFile].updateMany(files)
      } yield (updateCount, deleteCount)
      for {
        logger <- Slf4jLogger.create[F]
        (updateCount, deleteCount) <- deleteAndInsert.transact(xa)
        _ <- logger.info(s"$updateCount rows updated. $deleteCount rows deleted")
      } yield ()
    }
  }

sealed trait CoRow:
  val coRef: UUID
  val ioRef: UUID
  val sha256Checksum: Option[String]
  val sha1Checksum: Option[String]
  val md5Checksum: Option[String]


case class PreservicaCoRow(coRef: UUID, ioRef: UUID, sha256Checksum: Option[String], sha1Checksum: Option[String], md5Checksum: Option[String]) extends CoRow
case class OcflCoRow(coRef: UUID, ioRef: UUID, representationType: String, sha256Checksum: Option[String], sha1Checksum: Option[String], md5Checksum: Option[String]) extends CoRow