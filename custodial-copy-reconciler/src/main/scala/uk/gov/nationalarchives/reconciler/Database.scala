package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import doobie.free.connection.ConnectionIO
import doobie.implicits.*
import doobie.util.Write
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.Update
import fs2.Chunk
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.utils.Utils.given

import java.util.UUID

trait Database[F[_]]:
  def writeToPsTable(psCoRowChunk: Chunk[PreservicaCoRow]): F[Unit]
  def writeToOcflTable(ocflCoRowChunk: Chunk[OcflCoRow]): F[Unit]
  def findAllMissingFiles(): F[List[String]]

object Database:

  def apply[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def writeToOcflTable(ocflCoRowChunk: Chunk[OcflCoRow]): F[Unit] = {
      val insertSql =
        "insert into OcflRows (coRef, ioRef, sha256Checksum) values (?, ?, ?)"
      writeTransaction("OcflRows", Update[OcflCoRow](insertSql).updateMany(ocflCoRowChunk))
    }

    override def writeToPsTable(psCoRows: Chunk[PreservicaCoRow]): F[Unit] = {
      val insertSql = "insert into PsRows (coRef, ioRef, sha256Checksum) values (?, ?, ?)"
      writeTransaction("PsRows", Update[PreservicaCoRow](insertSql).updateMany(psCoRows))
    }

    override def findAllMissingFiles(): F[List[String]] =
      for {
        psMissingFromOcfl <- findPsFileMissingFromOcfl()
        ocflMissingFromPs <- findOcflFileMissingFromPs()
      } yield psMissingFromOcfl ++ ocflMissingFromPs

    private def findPsFileMissingFromOcfl(): F[List[String]] = {
      val selectSql = sql"select p.* from PsRows p LEFT JOIN OcflRows o on p.sha256checksum = o.sha256Checksum WHERE o.sha256Checksum is null;"
      selectSql.query[PreservicaCoRow].to[List].transact(xa).flatMap { psRefs =>
        for {
          logger <- Slf4jLogger.create[F]
          messages <- psRefs.traverse { row =>
            val message = s"CO ${row.coRef} (parent: ${row.ioRef}) is in Preservica, but its checksum could not be found in CC"
            logger.warn(message).map(_ => message)
          }
        } yield messages
      }
    }

    private def findOcflFileMissingFromPs(): F[List[String]] = {
      val selectSql = sql"select o.* from OcflRows o LEFT JOIN PsRows p on p.sha256checksum = o.sha256Checksum where p.sha256Checksum is null"
      selectSql.query[OcflCoRow].to[List].transact(xa).flatMap { ocflRefs =>
        for {
          logger <- Slf4jLogger.create[F]
          messages <- ocflRefs.traverse { row =>
            val message = s"CO ${row.coRef} (parent: ${row.ioRef}) is in CC, but its checksum could not be found in Preservica"
            logger.warn(message).map(_ => message)
          }
        } yield messages
      }
    }

    private def writeTransaction(tableName: String, connection: ConnectionIO[Int]) = for {
      logger <- Slf4jLogger.create[F]
      updateCount <- connection.transact(xa)
      _ <- logger.info(s"$tableName: $updateCount rows updated")
    } yield ()
  }

sealed trait CoRow:
  val coRef: UUID
  val ioRef: UUID
  val sha256Checksum: Option[String]

case class PreservicaCoRow(
    coRef: UUID,
    ioRef: UUID,
    sha256Checksum: Option[String]
) extends CoRow
case class OcflCoRow(
    coRef: UUID,
    ioRef: UUID,
    sha256Checksum: Option[String]
) extends CoRow
