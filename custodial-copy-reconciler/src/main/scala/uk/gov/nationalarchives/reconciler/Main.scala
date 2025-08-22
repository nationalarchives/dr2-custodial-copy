package uk.gov.nationalarchives.reconciler

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
import uk.gov.nationalarchives.dp.client.{Entities, EntityClient}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.ContentObject
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.reconciler.Configuration.impl
import uk.gov.nationalarchives.reconciler.OcflService
import uk.gov.nationalarchives.utils.Detail
import uk.gov.nationalarchives.utils.DetailType.DR2DevMessage

import java.net.URI
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.*

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

  private def logError(err: Throwable) = for
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  yield ()

  private def logComplete(count: Int) = for
    logger <- Slf4jLogger.create[IO]
    _ <- logger.info(Map("count" -> count.toString, "timestamp" -> Instant.now.getEpochSecond.toString))("CC reconcile complete")
  yield ()

  override def runtimeConfig: IORuntimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInitialDelay = Duration.Inf)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      client <- Fs2Client.entityClient(
        config.preservicaSecretName,
        potentialProxyUrl = config.proxyUrl,
        retryCount = 10
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
    val builder = Builder[IO](client)
    val endDate = ZonedDateTime.now(ZoneId.systemDefault())

    val ocfl = ocflService.getAllObjectFiles
      .chunkN(10000)
      .evalTap(database.writeToOcflCOsTable)
      .compile
      .drain

    val startOfEpoch = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0), ZoneId.systemDefault())
    def updatedSince(start: Int): IO[Seq[Entities.Entity]] = client.entitiesUpdatedSince(startOfEpoch, start, potentialEndDate = Option(endDate))

    def getEntities(start: Int = 0): Stream[IO, CoRow] =
      Stream
        .unfoldEval(0) { start =>
          updatedSince(start).flatMap {
            case Nil => IO.none
            case entities =>
              entities
                .filter(e => e.entityType.contains(ContentObject) && !e.deleted)
                .groupBy(_.ref)
                .keys
                .toList
                .grouped(configuration.config.maxConcurrency)
                .toList
                .parFlatTraverse(builder.run)
                .map(rows => (rows, start + 1000).some)
          }
        }
        .flatMap(Stream.emits)

    val ps = getEntities()
      .chunkN(1000)
      .evalTap(database.writeToPreservicaCOsTable)
      .compile
      .drain

    IO.both(ocfl, ps) >> database.findAllMissingCOs().flatMap { missingCOs =>
      logComplete(missingCOs.size) >>
        IO.whenA(missingCOs.nonEmpty) {
          if missingCOs.size > 10 then
            sendMissingCosToSlack(List("More than 10 missing Content Objects have been detected. Check the CC Reconciler logs for details."))
          else sendMissingCosToSlack(missingCOs)
        }
    }
  }
}
