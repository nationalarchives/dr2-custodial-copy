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
import fs2.Chunk
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.reconciler.Database.TableName.{OcflCOs, PreservicaCOs}
import uk.gov.nationalarchives.utils.Utils.given

import java.util.UUID

trait Database[F[_]]:
  def writeToPreservicaCOsTable(cosInPS: Chunk[PreservicaCoRow]): F[Unit]
  def writeToOcflCOsTable(expectedCosInPS: Chunk[OcflCoRow]): F[Unit]
  def findAllMissingCOs(): F[List[String]]

object Database:
  enum TableName:
    case OcflCOs, PreservicaCOs

  def apply[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def writeToOcflCOsTable(expectedCosInPS: Chunk[OcflCoRow]): F[Unit] = {
      val insertSql = s"insert into OcflCOs (coRef, ioRef, sha256Checksum) values (?, ?, ?)"
      writeTransaction(OcflCOs, Update[OcflCoRow](insertSql).updateMany(expectedCosInPS))
    }

    override def writeToPreservicaCOsTable(cosInPS: Chunk[PreservicaCoRow]): F[Unit] = {
      val insertSql = s"insert into PreservicaCOs (coRef, ioRef, sha256Checksum) values (?, ?, ?)"
      writeTransaction(PreservicaCOs, Update[PreservicaCoRow](insertSql).updateMany(cosInPS))
    }

    override def findAllMissingCOs(): F[List[String]] =
      for {
        psCOsMissingFromOcfl <- findPsCOsMissingFromOcfl()
        ocflCOsMissingFromPs <- findOcflCOsMissingFromPs()
      } yield psCOsMissingFromOcfl ++ ocflCOsMissingFromPs

    private def findPsCOsMissingFromOcfl(): F[List[String]] = {
      val selectSql = sql"select p.* from PreservicaCOs p LEFT JOIN OcflCOs o on p.sha256checksum = o.sha256Checksum WHERE o.sha256Checksum is null;"
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

    private def findOcflCOsMissingFromPs(): F[List[String]] = {
      val selectSql = sql"select o.* from OcflCOs o LEFT JOIN PreservicaCOs p on p.sha256checksum = o.sha256Checksum where p.sha256Checksum is null"
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

    private def writeTransaction(tableName: TableName, connection: ConnectionIO[Int]) = for {
      logger <- Slf4jLogger.create[F]
      updateCount <- connection.transact(xa)
      _ <- logger.info(s"$tableName: $updateCount rows updated.")
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
