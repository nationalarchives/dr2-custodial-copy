package uk.gov.nationalarchives.reconciler

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.*
import pureconfig.module.catseffect.syntax.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DAEventBridgeClient
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.reconciler.Configuration.impl
import uk.gov.nationalarchives.reconciler.OcflService
import uk.gov.nationalarchives.utils.Detail
import uk.gov.nationalarchives.utils.DetailType.DR2DevMessage

import java.net.URI
import java.util.UUID

object Main extends IOApp {
  case class Config(
      preservicaSecretName: String,
      databasePath: String,
      maxConcurrency: Int,
      ocflRepoDir: String,
      ocflWorkDir: String,
      proxyUrl: Option[URI] = None
  ) derives ConfigReader
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
      eventBridgeClient = DAEventBridgeClient[IO]()
      _ <- runReconciler(client, OcflService(config), eventBridgeClient).handleErrorWith(logError)
    } yield ExitCode.Success

  def runReconciler(client: EntityClient[IO, Fs2Streams[IO]], ocflService: OcflService[IO], eventBridgeClient: DAEventBridgeClient[IO])(using
      configuration: Configuration
  ): IO[Unit] = {
    def sendMissingCosToSlack(missingCoMessages: List[String]): IO[Unit] =
      missingCoMessages.traverse(message => eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2DevMessage, Detail(message))).void

    val database = Database[IO]

    client
      .streamAllEntityRefs()
      .collect {
        case coRef: ContentObjectRef     => coRef
        case ioRef: InformationObjectRef => ioRef
      }
      .chunkN(configuration.config.maxConcurrency)
      .parEvalMap(configuration.config.maxConcurrency) { entityRefChunks => Builder[IO].run(client, ocflService, entityRefChunks) }
      .evalTap { coRowChunks =>
        val psCoRowChunks = coRowChunks.collect { case psCoRow: PreservicaCoRow => psCoRow }
        val ocflCoRowChunks = coRowChunks.collect { case ocflCoRow: OcflCoRow => ocflCoRow }
        database.writeToPreservicaCOsTable(psCoRowChunks) >> database.writeToOcflCOsTable(ocflCoRowChunks)
      }
      .compile
      .drain >> database.findAllMissingCOs().flatMap { missingCOs =>
      IO.whenA(missingCOs.nonEmpty) {
        if missingCOs.size > 10 then
          sendMissingCosToSlack(List("More than 10 missing Content Objects have been detected. Check the CC Reconciler logs for details."))
        else sendMissingCosToSlack(missingCOs)
      }
    }
  }
}
