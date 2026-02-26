package uk.gov.nationalarchives.webapp

import cats.data.{Kleisli, OptionT}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.*
import fs2.io.net.Network
import fs2.io.net.Network.forAsync
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.headers.Location
import org.http4s.implicits.*
import org.http4s.server.middleware.Logger
import org.http4s.server.staticcontent.resourceServiceBuilder
import org.http4s.*
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

object FrontEndServer:

  def run[F[_]: Async]: F[Nothing] = {

    given LoggerFactory[F] = Slf4jFactory.create[F]

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

    given Network[F] = forAsync

    val appF = resourceServiceBuilder[F]("/").toRoutes.map { resourceRoutes =>
      val httpApp = (
        handleErrors(FrontEndRoutes.ocflRoutes[F]) <+> resourceRoutes
      ).orNotFound

      Logger.httpApp(true, false)(httpApp)
    }

    for {
      app <- Resource.eval[F, HttpApp[F]](appF)
      a <-
        EmberServerBuilder
          .default[F]
          .withHost(ipv4"0.0.0.0")
          .withPort(port"8080")
          .withHttpApp(app)
          .build
    } yield ()
  }.useForever
