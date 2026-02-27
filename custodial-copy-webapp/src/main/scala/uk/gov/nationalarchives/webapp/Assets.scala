package uk.gov.nationalarchives.webapp

import cats.effect.Async
import cats.implicits.*
import doobie.Transactor
import doobie.implicits.*
import doobie.util.fragments.whereAndOpt
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor.Aux
import doobie.util.{Get, Put}
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigReader, ConfigSource}
import uk.gov.nationalarchives.utils.Utils.{OcflFile, given}
import uk.gov.nationalarchives.webapp.FrontEndRoutes.SearchResponse

import java.time.ZoneOffset
import java.util.UUID

trait Assets[F[_]]:
  def filePath(id: UUID): F[String]

  def findFiles(searchResponse: SearchResponse): F[List[OcflFile]]

object Assets:
  private case class Config(databasePath: String) derives ConfigReader

  def apply[F[_]](using ev: Assets[F]): Assets[F] = ev
  given instance[F[_]: Async]: Assets[F] = new Assets[F]:

    val loadXa: F[Aux[F, Unit]] = ConfigSource.default.loadF[F, Config]().map { config =>
      Transactor.fromDriverManager[F](
        driver = "org.sqlite.JDBC",
        url = s"jdbc:sqlite:${config.databasePath}",
        logHandler = Option(LogHandler.jdkLogHandler)
      )
    }

    override def filePath(id: UUID): F[String] = for {
      xa <- loadXa
      potentialPath <- sql"SELECT path from files where fileId = $id".query[String].to[List].transact(xa)
      path <- Async[F].fromOption(potentialPath.headOption, new RuntimeException(s"Id $id not found in the database"))
    } yield path

    override def findFiles(searchResponse: SearchResponse): F[List[OcflFile]] = {
      val idWhere = searchResponse.id.map(i => fr"id = $i")
      val zrefWhere = searchResponse.zref.map(z => fr"zref = $z")
      val citationWhere = searchResponse.citation.map(c => fr"citation = $c")
      val sourceIdWhere = searchResponse.sourceId.map(s => fr"sourceId = $s")
      val ingestDateTimeWhere = searchResponse.ingestDateTime.map { i =>
        val endOfDay = i.atOffset(ZoneOffset.UTC).toLocalDate.atTime(23, 59, 59).atZone(ZoneOffset.UTC).toInstant
        fr"ingestDateTime >= $i AND ingestDateTime <= $endOfDay"
      }
      val consignmentRefWhere = searchResponse.consignmentRef.map(c => fr"consignmentRef = $c")
      val query = fr"SELECT * from files" ++ whereAndOpt(idWhere, zrefWhere, citationWhere, sourceIdWhere, ingestDateTimeWhere, consignmentRefWhere)
      loadXa.flatMap { xa =>
        query.query[OcflFile].to[List].transact(xa)
      }
    }
