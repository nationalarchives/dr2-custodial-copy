package uk.gov.nationalarchives.webapp

import cats.effect.Async
import cats.implicits.*
import uk.gov.nationalarchives.webapp.FrontEndRoutes.SearchResponse
import uk.gov.nationalarchives.utils.Utils.OcflFile
import doobie.Transactor
import doobie.implicits.*
import doobie.util.fragments.whereAndOpt
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor.Aux
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*

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
      potentialPath <- sql"SELECT path from files where fileId = ${id.toString}".query[String].to[List].transact(xa)
      path <- Async[F].fromOption(potentialPath.headOption, new RuntimeException(s"Id $id not found in the database"))
    } yield path

    override def findFiles(searchResponse: SearchResponse): F[List[OcflFile]] = {
      val idWhere = searchResponse.id.map(i => fr"id = $i")
      val zrefWhere = searchResponse.zref.map(z => fr"zref = $z")
      val query = fr"SELECT * from files" ++ whereAndOpt(idWhere, zrefWhere)
      loadXa.flatMap { xa =>
        query.query[OcflFile].to[List].transact(xa)
      }
    }
