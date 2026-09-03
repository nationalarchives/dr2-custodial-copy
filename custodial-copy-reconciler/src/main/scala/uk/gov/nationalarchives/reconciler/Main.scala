package uk.gov.nationalarchives.reconciler

import cats.effect.std.Mutex
import cats.effect.unsafe.IORuntimeConfig
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
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.reconciler.Configuration.impl
import uk.gov.nationalarchives.reconciler.Database.{CoRow, Result}
import uk.gov.nationalarchives.reconciler.OcflService
import uk.gov.nationalarchives.utils.Detail
import uk.gov.nationalarchives.utils.DetailType.DR2DevMessage

import java.net.URI
import java.time.{Instant, OffsetDateTime}
import java.util.UUID
import scala.concurrent.duration.*

object Main extends IOApp {
  case class Config(
      preservicaSecretName: String,
      databasePath: String,
      maxConcurrency: Int,
      ocflRepoDir: String,
      ocflWorkDir: String,
      daysToIgnore: Int,
      proxyUrl: Option[URI] = None
  ) derives ConfigReader

  case class Message(id: UUID)

  given Decoder[Message] = (c: HCursor) =>
    for {
      id <- c.downField("ioRef").as[String]
    } yield Message(UUID.fromString(id))

  private def logError(err: Throwable) = for
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running Custodial Copy Reconciler")
  yield ()

  private def logCompletion(result: Result) = for
    logger <- Slf4jLogger.create[IO]
    _ <- logger.info(
      Map(
        "ccCOsCount" -> result.ccCOsCount.toString,
        "psCOsCount" -> result.psCOsCount.toString,
        "ccCOsMissingFromPs" -> result.ccCOsMissingFromPs.length.toString,
        "psCOsMissingFromCc" -> result.psCOsMissingFromCc.length.toString,
        "completionTimestamp" -> Instant.now.getEpochSecond.toString
      )
    )("CC reconcile complete")
  yield ()

  override def runtimeConfig: IORuntimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInitialDelay = Duration.Inf)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      client <- Fs2Client.entityClient(
        config.preservicaSecretName,
        13.minutes,
        potentialProxyUrl = config.proxyUrl,
        retryCount = 10
      )
      eventBridgeClient = DAEventBridgeClient[IO]()
      mutex <- Mutex[IO]
      _ <- runReconciler(client, OcflService(config), eventBridgeClient, Database[IO](mutex)).handleErrorWith(logError)
    } yield ExitCode.Success

  def runReconciler(
      client: EntityClient[IO, Fs2Streams[IO]],
      ocflService: OcflService[IO],
      eventBridgeClient: DAEventBridgeClient[IO],
      database: Database[IO]
  )(using
      configuration: Configuration
  ): IO[Unit] = {
    def sendMissingCosToSlack(missingCoMessages: List[String]): IO[Unit] =
      missingCoMessages.traverse(message => eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2DevMessage, Detail(message))).void

    val endDate = OffsetDateTime.now.minusDays(configuration.config.daysToIgnore)

    val builder = Builder[IO](client)

    val ocfl = ocflService.getAllObjectFiles
      .chunkN(10000)
      .evalTap(database.writeToOcflCOsTable)
      .compile
      .drain

    def getEntities: Stream[IO, CoRow] =
      client
        .getAllAssetIds(configuration.config.maxConcurrency)
        .chunkN(configuration.config.maxConcurrency)
        .flatMap(builder.run)

    val ps = getEntities
      .chunkN(1000)
      .evalTap(database.writeToPreservicaCOsTable)
      .compile
      .drain

    database.deleteFromTables() >> IO.both(ocfl, ps) >> database.findAllMissingCOs(endDate).flatMap { result =>
      val missingCOs = result.psCOsMissingFromCc ++ result.ccCOsMissingFromPs
      logCompletion(result) >>
        IO.whenA(missingCOs.nonEmpty) {
          if missingCOs.size > 10 then
            sendMissingCosToSlack(
              List(":alert-noflash-slow: More than 10 missing Content Objects have been detected. Check the CC Reconciler logs for details.")
            )
          else sendMissingCosToSlack(missingCOs)
        }
    }
  }
}
