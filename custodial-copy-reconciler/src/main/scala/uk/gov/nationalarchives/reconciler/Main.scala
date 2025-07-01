package uk.gov.nationalarchives.reconciler

import cats.effect.std.Semaphore
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.*
import Configuration.impl
import fs2.{Chunk, Stream}
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigReader, ConfigSource}
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef, StructuralObjectRef}
import uk.gov.nationalarchives.dp.client.{Entities, EntityClient}
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.{Access, Preservation}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.custodialcopy.OcflService
import uk.gov.nationalarchives.custodialcopy

import java.net.URI
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

object Main extends IOApp {
  case class Config(preservicaSecretName: String, databasePath: String, maxConcurrency: Int, ocflRepoDir: String, ocflWorkDir: String, proxyUrl: Option[URI] = None) derives ConfigReader
  case class Message(id: UUID)

  given Decoder[Message] = (c: HCursor) =>
    for {
      id <- c.downField("ioRef").as[String]
    } yield Message(UUID.fromString(id))

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      client <- Fs2Client.entityClient(
        config.preservicaSecretName,
        potentialProxyUrl = config.proxyUrl
      )
      _ <- IO(runReconciler(client).handleErrorWith(err => Stream.eval(logError(err))))
    } yield ExitCode.Success

  def runReconciler(client: EntityClient[IO, Fs2Streams[IO]])(using configuration: Configuration) =
    client
      .streamAllEntityRefs()
      .filterNot { case so: StructuralObjectRef => true }
      .chunkN(50)
      .parEvalMap(configuration.config.maxConcurrency)( Builder[IO].run )
      .parJoin(configuration.config.maxConcurrency)
//        val streamOfCoRows = coRowChunks.map(coRowChunk => Stream.chunk(coRowChunk.flatten))
//        streamOfCoRows
      }

    //

}
