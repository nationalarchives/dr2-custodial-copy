package uk.gov.nationalarchives.reconciler

import cats.effect.Async
import cats.implicits.*
import doobie.free.connection.ConnectionIO
import doobie.implicits.*
import doobie.util.Write
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.{Query0, Update}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.reconciler.Database.TableName.{ActualCosInPS, ExpectedCosInPS}
import uk.gov.nationalarchives.utils.Utils.given

import java.util.UUID

trait Database[F[_]]:
  def writeToActuallyInPsTable(cosInPS: List[PreservicaCoRow]): F[Unit]
  def writeToExpectedInPsTable(expectedCosInPS: List[OcflCoRow]): F[Unit]
  def updateCoInPSWithCCStatus(coInPS: PreservicaCoRow): F[Unit]

object Database:
  enum TableName:
    case ExpectedCosInPS, ActualCosInPS

  def apply[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def writeToExpectedInPsTable(expectedCosInPS: List[OcflCoRow]): F[Unit] = {
      val deleteSql = s"delete from $ExpectedCosInPS where coRef = ?"
      val insertSql = s"insert into $ExpectedCosInPS (ioRef, coRef, representationType, sha256Checksum, sha1Checksum, md5Checksum) values (?, ?, ?, ?, ?, ?)"

      val deleteAndInsert: ConnectionIO[(Int, Int)] = for {
        deleteCount <- Update[UUID](deleteSql).updateMany(expectedCosInPS.map(_.coRef))
        updateCount <- Update[OcflCoRow](insertSql).updateMany(expectedCosInPS)
      } yield (updateCount, deleteCount)

      writeTransaction(ExpectedCosInPS, deleteAndInsert)
    }

    override def writeToActuallyInPsTable(cosInPS: List[PreservicaCoRow]): F[Unit] = {
      val deleteSql = s"delete from $ActualCosInPS where coRef = ?"
      val insertSql = s"insert into $ActualCosInPS (ioRef, coRef, sha256Checksum, sha1Checksum, md5Checksum, inCC) values (?, ?, ?, ?, ?)"

      val deleteAndInsert: ConnectionIO[(Int, Int)] = for {
        deleteCount <- Update[UUID](deleteSql).updateMany(cosInPS.map(_.coRef))
        updateCount <- Update[PreservicaCoRow](insertSql).updateMany(cosInPS)
      } yield (updateCount, deleteCount)

      writeTransaction(ActualCosInPS, deleteAndInsert)
    }

    def updateCoInPSWithCCStatus(coInPS: PreservicaCoRow): F[Unit] = {
      val selectSql = s"select from $ExpectedCosInPS (coRef) values ? where coRef = ?"
      val deleteSql = s"delete from $ActualCosInPS where coRef = ?"
      val insertSql = s"insert into $ActualCosInPS (ioRef, coRef, sha256Checksum, sha1Checksum, md5Checksum, inCC) values (?, ?, ?, ?, ?)"

      val selectDeleteAndInsert: ConnectionIO[(Int, Int)] = for {
        coInPSUpdated <- Query0[OcflCoRow](selectSql).to[List].map{
          ocflRow => coInPS.copy(inCC = Some(if ocflRow.isEmpty then false else true))
        }
        deleteCount <- Update[UUID](deleteSql).updateMany(List(coInPSUpdated.coRef))
        updateCount <- Update[PreservicaCoRow](selectSql).updateMany(List(coInPSUpdated))
      } yield (updateCount, deleteCount)

      writeTransaction(ActualCosInPS, selectDeleteAndInsert)
    }

    private def writeTransaction(tableName: TableName, connection: ConnectionIO[(Int, Int)]) = for {
      logger <- Slf4jLogger.create[F]
      (updateCount, deleteCount) <- connection.transact(xa)
      _ <- logger.info(s"$tableName: $updateCount rows updated. $deleteCount rows deleted")
    } yield ()
  }

sealed trait CoRow:
  val coRef: UUID
  val ioRef: UUID
  val generationType: String
  val sha256Checksum: Option[String]
  val sha1Checksum: Option[String]
  val md5Checksum: Option[String]


case class PreservicaCoRow(coRef: UUID, ioRef: UUID, generationType: String, sha256Checksum: Option[String], sha1Checksum: Option[String], md5Checksum: Option[String], inCC: Option[Boolean]=None) extends CoRow
case class OcflCoRow(coRef: UUID, ioRef: UUID, representationType: String, generationType: String, sha256Checksum: Option[String], sha1Checksum: Option[String], md5Checksum: Option[String]) extends CoRow