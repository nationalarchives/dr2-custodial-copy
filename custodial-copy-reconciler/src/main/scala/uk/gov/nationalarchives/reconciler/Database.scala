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
import uk.gov.nationalarchives.reconciler.Database.TableName.{PreservicaCOs, OcflCOs}
import uk.gov.nationalarchives.utils.Utils.given

import java.util.UUID

trait Database[F[_]]:
  def writeToPreservicaCOsTable(cosInPS: List[PreservicaCoRow]): F[Unit]
  def writeToOcflCOsTable(expectedCosInPS: List[OcflCoRow]): F[Unit]
  def checkIfPsCoInCc(coInPS: PreservicaCoRow): F[List[String]]
  def checkIfCcCoInPs(coInCC: OcflCoRow): F[List[String]]

object Database:
  enum TableName:
    case OcflCOs, PreservicaCOs

  def apply[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def writeToOcflCOsTable(expectedCosInPS: List[OcflCoRow]): F[Unit] = {
      val insertSql = s"insert into OcflCOs (coRef, ioRef, sha256Checksum) values (?, ?, ?)"
      writeTransaction(OcflCOs, Update[OcflCoRow](insertSql).updateMany(expectedCosInPS))
    }

    override def writeToPreservicaCOsTable(cosInPS: List[PreservicaCoRow]): F[Unit] = {
      val insertSql = s"insert into PreservicaCOs (coRef, ioRef, sha256Checksum) values (?, ?, ?)"
      writeTransaction(PreservicaCOs, Update[PreservicaCoRow](insertSql).updateMany(cosInPS))
    }

    def checkIfPsCoInCc(coInPS: PreservicaCoRow): F[List[String]] = {
      val selectSql = checkIfChecksumInTableStatement(coInPS, OcflCOs)
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
      val selectSql = checkIfChecksumInTableStatement(coInCC, PreservicaCOs)
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
         | and sha256Checksum = '${coRow.sha256Checksum.getOrElse("N/A")}'""".stripMargin

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
