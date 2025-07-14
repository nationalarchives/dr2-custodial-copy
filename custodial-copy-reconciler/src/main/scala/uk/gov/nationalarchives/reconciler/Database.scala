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
  def checkIfPsCoInCc(coInPS: PreservicaCoRow): F[List[String]]
  def checkIfCcCoInPs(coInCC: OcflCoRow): F[List[String]]

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
      val deleteSql = "delete from ExpectedCosInPS where coRef = ?"
      val insertSql =
        "insert into ExpectedCosInPS (coRef, ioRef, representationType, generationType, sha256Checksum, sha1Checksum, md5Checksum) values (?, ?, ?, ?, ?, ?, ?)"

      val deleteAndInsert: ConnectionIO[(Int, Int)] = for {
        deleteCount <- Update[UUID](deleteSql).updateMany(expectedCosInPS.map(_.coRef))
        updateCount <- Update[OcflCoRow](insertSql).updateMany(expectedCosInPS)
      } yield (updateCount, deleteCount)

      writeTransaction(ExpectedCosInPS, deleteAndInsert)
    }

    override def writeToActuallyInPsTable(cosInPS: List[PreservicaCoRow]): F[Unit] = {
      val deleteSql = s"delete from ActualCosInPS where coRef = ?"
      val insertSql = "insert into ActualCosInPS (coRef, ioRef, generationType, sha256Checksum, sha1Checksum, md5Checksum) values (?, ?, ?, ?, ?, ?)"

      val deleteAndInsert: ConnectionIO[(Int, Int)] = for {
        deleteCount <- Update[UUID](deleteSql).updateMany(cosInPS.map(_.coRef))
        updateCount <- Update[PreservicaCoRow](insertSql).updateMany(cosInPS)
      } yield (updateCount, deleteCount)

      writeTransaction(ActualCosInPS, deleteAndInsert)
    }

    def checkIfPsCoInCc(coInPS: PreservicaCoRow): F[List[String]] = {
      val selectSql = checkIfChecksumInTableStatement(coInPS, ExpectedCosInPS)
      val selectCoRow: ConnectionIO[List[String]] = for {
        ocflCoRows <- Query0[String](selectSql).to[List]
      } yield
        if ocflCoRows.isEmpty then
          List(
            s"CO ${coInPS.coRef} (parent: ${coInPS.ioRef}) is in Preservica, but its checksum could not be found in CC"
          )
        else Nil

      selectCoRow.transact(xa)
    }

    def checkIfCcCoInPs(coInCC: OcflCoRow): F[List[String]] = {
      val selectSql = checkIfChecksumInTableStatement(coInCC, ActualCosInPS)
      val selectCoRow: ConnectionIO[List[String]] = for {
        preservicaCoRows <- Query0[PreservicaCoRow](selectSql).to[List]
      } yield
        if preservicaCoRows.isEmpty then
          List(
            s"CO ${coInCC.coRef} (parent: ${coInCC.ioRef}) is in CC, but its checksum could not be found in Preservica"
          )
        else Nil
      selectCoRow.transact(xa)
    }

    private def checkIfChecksumInTableStatement(coRow: CoRow, table: TableName): String =
      s"""select * from $table coRef
         | where coRef = '${coRow.coRef}'
         | and (
         |   sha256Checksum = '${coRow.sha256Checksum.getOrElse("N/A")}'
         |   or sha1Checksum = '${coRow.sha1Checksum.getOrElse("N/A")}'
         |   or md5Checksum = '${coRow.md5Checksum.getOrElse("N/A")}'
         | )""".stripMargin

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

case class PreservicaCoRow(
    coRef: UUID,
    ioRef: UUID,
    generationType: String,
    sha256Checksum: Option[String],
    sha1Checksum: Option[String],
    md5Checksum: Option[String]
) extends CoRow
case class OcflCoRow(
    coRef: UUID,
    ioRef: UUID,
    representationType: String,
    generationType: String,
    sha256Checksum: Option[String],
    sha1Checksum: Option[String],
    md5Checksum: Option[String]
) extends CoRow
