package uk.gov.nationalarchives.webapp

import cats.data.{Kleisli, OptionT}
import cats.effect.Async
import cats.syntax.all.*
import com.comcast.ip4s.*
import html.error
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.headers.Location
import org.http4s.implicits.*
import org.http4s.server.middleware.Logger
import org.http4s.server.staticcontent.resourceServiceBuilder
import org.http4s.{HttpRoutes, Query, Response, Status, Uri}
import uk.gov.nationalarchives.webapp.FrontEndRoutes.*

object FrontEndServer:

  def run[F[_]: Async]: F[Nothing] = {

    def handleError(err: Throwable) = OptionT.liftF {
      Async[F].pure {
        Response[F]()
          .withStatus(Status.Found)
          .withHeaders(
            Location(
              Uri(path = Uri.Path.apply(Vector(Uri.Path.Segment("error"))), query = Query.fromVector(Vector(("message", Option(err.getMessage)))))
            )
          )
      }
    }

    def handleErrors(routes: HttpRoutes[F]): HttpRoutes[F] =
      Kleisli { req =>
        routes(req).handleErrorWith(handleError)
      }

    val httpApp = (
      handleErrors(FrontEndRoutes.ocflRoutes[F]) <+> resourceServiceBuilder[F]("/").toRoutes
    ).orNotFound

    val finalHttpApp = Logger.httpApp(true, false)(httpApp)

    for {
      _ <-
        EmberServerBuilder
          .default[F]
          .withHost(ipv4"0.0.0.0")
          .withPort(port"8080")
          .withHttpApp(finalHttpApp)
          .build
    } yield ()
  }.useForever
