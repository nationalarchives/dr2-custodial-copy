package uk.gov.nationalarchives.reconciler

import cats.effect.std.Semaphore
import cats.effect.{ExitCode, IO, IOApp}
import fs2.{Chunk, Stream}
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigReader, ConfigSource}
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.custodialcopy.OcflService
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.reconciler.Configuration.impl
import uk.gov.nationalarchives.utils.OcflServiceConfig

import java.net.URI
import java.util.UUID

object Main extends IOApp {
  case class Config(preservicaSecretName: String, databasePath: String, maxConcurrency: Int, repoDir: String, workDir: String, proxyUrl: Option[URI] = None) extends OcflServiceConfig derives ConfigReader
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

  def runReconciler(client: EntityClient[IO, Fs2Streams[IO]])(using configuration: Configuration) = {
    val database = Database[IO]
    client
      .streamAllEntityRefs()
      .filter {
        case _: InformationObjectRef | _: ContentObjectRef => true
        case _ => false
      }
      .asInstanceOf[Stream[IO, InformationObjectRef | ContentObjectRef]]
      .chunkN(50)
      .parEvalMap(configuration.config.maxConcurrency) { entityRefChunks =>
        for {
          config <- ConfigSource.default.loadF[IO, Config]()
          semaphore <- Semaphore[IO](1)
          ocflService <- OcflService(config, semaphore)
          coRowChunks <- Builder[IO].run(client, ocflService, entityRefChunks)
        } yield Stream.chunk(coRowChunks)
      }
      .parJoin(configuration.config.maxConcurrency)
      .evalTap {
        case preservicaCoRows: List[PreservicaCoRow] => database.writeToActuallyInPsTable(preservicaCoRows)
        case ocflCoRows: List[OcflCoRow] => database.writeToExpectedInPsTable(ocflCoRows)
        case _ => IO.unit
      }.map {
        case preservicaCoRows: List[PreservicaCoRow] => preservicaCoRows.map(database.updateCoInPSWithCCStatus)
      }
  }
}
