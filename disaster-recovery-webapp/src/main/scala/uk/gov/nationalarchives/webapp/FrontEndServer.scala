package uk.gov.nationalarchives.webapp

import cats.effect.Async
import cats.syntax.all.*
import com.comcast.ip4s.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.http4s.server.middleware.Logger
import org.http4s.server.staticcontent.resourceServiceBuilder

object FrontEndServer:

  def run[F[_]: Async]: F[Nothing] = {
    val httpApp = (
      FrontEndRoutes.ocflRoutes[F] <+> resourceServiceBuilder[F]("/").toRoutes
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
