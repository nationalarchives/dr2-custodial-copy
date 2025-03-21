package uk.gov.nationalarchives.webapp

import cats.data.EitherT
import cats.effect.*
import cats.implicits.*
import fs2.io.file.Files
import fs2.io.file.Files.forAsync
import html.{error, results, search}
import org.http4s.*
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.io.{OptionalQueryParamDecoderMatcher, QueryParamDecoderMatcher}
import org.http4s.headers.*

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

object FrontEndRoutes:

  extension [F[_]](response: Response[F]) private def asHtml: Response[F] = response.putHeaders(`Content-Type`(MediaType.text.html))

  extension (form: UrlForm)
    private def value(name: String): Option[String] =
      form.values.get(name).flatMap(_.headOption).flatMap {
        case ""   => None
        case rest => Some(rest)
      }

    private def toInstant: Option[Instant] =
      for {
        day <- form.value("ingest-date-day").map(_.toInt)
        month <- form.value("ingest-date-month").map(_.toInt)
        year <- form.value("ingest-date-year").map(_.toInt)
      } yield LocalDate.of(year, month, day).atStartOfDay(ZoneOffset.UTC).toInstant

    private def generateSearchResponse: SearchResponse =
      SearchResponse(
        form.value("id").map(UUID.fromString),
        form.value("zref"),
        form.value("sourceId"),
        form.value("citation"),
        form.toInstant,
        form.value("consignmentRef")
      )

  given QueryParamDecoder[UUID] = QueryParamDecoder[String].map(UUID.fromString)
  given QueryParamDecoder[Instant] = QueryParamDecoder[String].map(_.toLong).map(Instant.ofEpochMilli)
  private object IdQueryParam extends OptionalQueryParamDecoderMatcher[UUID]("id")
  private object ZrefQueryParam extends OptionalQueryParamDecoderMatcher[String]("zref")
  private object SourceIdQueryParam extends OptionalQueryParamDecoderMatcher[String]("sourceId")
  private object CitationQueryParam extends OptionalQueryParamDecoderMatcher[String]("citation")
  private object IngestDateQueryParam extends OptionalQueryParamDecoderMatcher[Instant]("ingestDate")
  private object ErrorMessageQueryParam extends QueryParamDecoderMatcher[String]("message")
  private object ConsignmentRefQueryParam extends OptionalQueryParamDecoderMatcher[String]("consignmentRef")

  case class SearchResponse(id: Option[UUID], zref: Option[String], sourceId: Option[String], citation: Option[String], ingestDateTime: Option[Instant], consignmentRef: Option[String]) {
    private val params: Vector[(String, Option[String])] = Vector(
      ("id", id.map(_.toString)),
      ("zref", zref),
      ("sourceId", sourceId),
      ("citation", citation),
      ("ingestDate", ingestDateTime.map(_.toEpochMilli.toString)),
      ("consignmentRef", consignmentRef)
    ).filter(_._2.nonEmpty)

    def toQuery: Query = Query.fromVector(params)
  }

  def ocflRoutes[F[_]: Async: Assets]: HttpRoutes[F] =
    val dsl = new Http4sDsl[F] {}
    import dsl.*

    given Files[F] = forAsync

    given EntityDecoder[F, SearchResponse] = EntityDecoder.decodeBy[F, SearchResponse](MediaType.application.`x-www-form-urlencoded`) { msg =>
      EitherT {
        msg.as[UrlForm].map(_.generateSearchResponse.asRight)
      }
    }

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
      case req @ GET -> Root / "search" :? IdQueryParam(id) +& ZrefQueryParam(zref) +& SourceIdQueryParam(sourceId) +& CitationQueryParam(
            citation
          ) +& IngestDateQueryParam(ingestDate) +& ConsignmentRefQueryParam(consignmentRef) =>
        for {
          files <- Assets[F].findFiles(SearchResponse(id, zref, sourceId, citation, ingestDate, consignmentRef))
          resp <- Ok(results(files).body)
        } yield resp.asHtml
      case req @ POST -> Root / "search" =>
        for {
          searchResponse <- req.as[SearchResponse]
          files <- Assets[F].findFiles(searchResponse)
        } yield Response[F]()
          .withStatus(Status.Found)
          .withHeaders(Location(Uri.apply(path = Uri.Path.apply(Vector(Uri.Path.Segment("search"))), query = searchResponse.toQuery)))
      case GET -> Root / "error" :? ErrorMessageQueryParam(errorMessage) =>
        InternalServerError(error(errorMessage).body).map(_.asHtml)

    }
