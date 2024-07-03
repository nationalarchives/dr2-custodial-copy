package uk.gov.nationalarchives.webapp

import cats.effect.*
import cats.implicits.*
import fs2.io.file.Files
import html.{results, search}
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.*
import org.http4s.*

import java.util.UUID

object FrontEndRoutes:

  extension [F[_]](response: Response[F]) private def asHtml: response.Self = response.putHeaders(`Content-Type`(MediaType.text.html))

  extension (form: UrlForm)
    def value(name: String): Option[String] = {
      form.values.get(name).flatMap(_.headOption).flatMap {
        case ""   => None
        case rest => Some(rest)
      }
    }

  case class SearchResponse(id: Option[String], zref: Option[String])

  def ocflRoutes[F[_]: Async: Assets]: HttpRoutes[F] =
    val dsl = new Http4sDsl[F] {}
    import dsl.*

    HttpRoutes.of[F] {
      case GET -> Root / "download" / UUIDVar(id) =>
        Assets[F].filePath(id).flatMap { path =>
          Ok(Files[F].readAll(fs2.io.file.Path(path)))
            .map(_.putHeaders(`Content-Type`(MediaType.application.`octet-stream`)))
        }
      case GET -> Root =>
        for {
          resp <- Ok(search().body)
        } yield resp.asHtml
      case req @ POST -> Root / "search" =>
        for {
          form <- req.as[UrlForm]
          files <- Assets[F].findFiles(SearchResponse(form.value("id"), form.value("zref")))
          resp <- Ok(results(files).body)
        } yield resp.asHtml
    }
