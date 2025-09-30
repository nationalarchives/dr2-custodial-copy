package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import doobie.Update
import doobie.free.connection.ConnectionIO
import doobie.implicits.*
import doobie.util.{Get, Put, Read, Write}
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import fs2.Chunk
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.reconciler.Database.TableName.{OcflCOs, PreservicaCOs}
import uk.gov.nationalarchives.utils.Utils.given

import java.util.UUID

trait Database[F[_]]:
  def writeToPreservicaCOsTable(cosInPS: Chunk[CoRow]): F[Unit]
  def writeToOcflCOsTable(expectedCosInPS: Chunk[CoRow]): F[Unit]
  def findAllMissingCOs(): F[Result]
  def deleteFromTables(): F[Unit]

object Database:
  enum TableName:
    case OcflCOs, PreservicaCOs

  def apply[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    given Put[EntityType] = Put[String].contramap(_.entityTypeShort)
    given Get[EntityType] = Get[String].map {
      case "SO" => StructuralObject
      case "IO" => InformationObject
      case "CO" => ContentObject
    }
    given Write[EntityType] = Write.fromPut
    given Read[EntityType] = Read.fromGet

    override def writeToOcflCOsTable(expectedCosInPS: Chunk[CoRow]): F[Unit] = {
      val insertSql = s"insert into OcflCOs (id, parent, sha256Checksum) values (?, ?, ?)"
      writeTransaction(OcflCOs, Update[CoRow](insertSql).updateMany(expectedCosInPS))
    }

    override def writeToPreservicaCOsTable(cosInPS: Chunk[CoRow]): F[Unit] = {
      val insertSql = s"insert into PreservicaCOs (id, parent, sha256Checksum) values (?, ?, ?)"
      writeTransaction(PreservicaCOs, Update[CoRow](insertSql).updateMany(cosInPS))
    }

    override def findAllMissingCOs(): F[Result] =
      for {
        psCOsCount <- preservicaCOsCount
        ccCOsCount <- ccCOsCount
        psCOsMissingFromCc <- findPsCOsMissingFromOcfl()
        ccCOsMissingFromPs <- findOcflCOsMissingFromPs()
      } yield Result(psCOsCount, psCOsMissingFromCc.distinct, ccCOsCount, ccCOsMissingFromPs.distinct)

    private def findPsCOsMissingFromOcfl(): F[List[String]] = {
      val selectSql = sql"select p.* from PreservicaCOs p LEFT JOIN OcflCOs o on p.sha256checksum = o.sha256Checksum WHERE o.sha256Checksum is null;"
      selectSql.query[CoRow].to[List].transact(xa).flatMap { psRefs =>
        for {
          logger <- Slf4jLogger.create[F]
          messages <- psRefs.traverse { row =>
            val message = s"CO ${row.id} is in Preservica, but its checksum could not be found in CC"
            logger.warn(message).map(_ => s":alert-noflash-slow: $message")
          }
        } yield messages
      }
    }

    private def findOcflCOsMissingFromPs(): F[List[String]] = {
      val selectSql = sql"select o.* from OcflCOs o LEFT JOIN PreservicaCOs p on p.sha256checksum = o.sha256Checksum where p.sha256Checksum is null"
      selectSql.query[CoRow].to[List].transact(xa).flatMap { ocflRefs =>
        for {
          logger <- Slf4jLogger.create[F]
          messages <- ocflRefs.traverse { row =>
            val message = s"CO ${row.id} is in CC, but its checksum could not be found in Preservica"
            logger.warn(message).map(_ => s":alert-noflash-slow: $message")
          }
        } yield messages
      }
    }

    private def preservicaCOsCount: F[Int] = {
      val selectSql = sql"select count(*) from PreservicaCOs;".query[Int].unique
      selectSql.transact(xa)
    }

    private def ccCOsCount: F[Int] = {
      val selectSql = sql"select count(*) from OcflCOs;".query[Int].unique
      selectSql.transact(xa)
    }

    private def writeTransaction(tableName: TableName, connection: ConnectionIO[Int]) = for {
      logger <- Slf4jLogger.create[F]
      updateCount <- connection.transact(xa)
      _ <- logger.info(s"$tableName: $updateCount rows updated.")
    } yield ()

    override def deleteFromTables(): F[Unit] =
      val deleteUpdates = for
        _ <- sql"delete from OcflCOs;".update.run
        _ <- sql"delete from PreservicaCOs;".update.run
      yield ()
      deleteUpdates.transact(xa)
  }

case class CoRow(
    id: UUID,
    parent: Option[UUID],
    sha256Checksum: Option[String]
)

case class Result(
    psCOsCount: Int,
    psCOsMissingFromCc: List[String],
    ccCOsCount: Int,
    ccCOsMissingFromPs: List[String]
)
