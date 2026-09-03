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
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.ContentObject
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.reconciler.Configuration.impl
import uk.gov.nationalarchives.reconciler.Database.{CoRow, Result}
import uk.gov.nationalarchives.reconciler.OcflService
import uk.gov.nationalarchives.utils.Detail
import uk.gov.nationalarchives.utils.DetailType.DR2DevMessage

import java.net.URI
import java.time.{Instant, LocalDate, LocalTime, ZoneId, ZonedDateTime}
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
      proxyUrl: Option[URI] = None,
      entitiesUpdatedSinceWindowDays: Int = 10,
      entitiesUpdatedSinceConcurrency: Int = 20
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
    val endDate = ZonedDateTime.now(ZoneId.systemDefault()).minusDays(configuration.config.daysToIgnore)

    val ocfl = ocflService.getAllObjectFiles
      .chunkN(10000)
      .evalTap(database.writeToOcflCOsTable)
      .compile
      .drain

    val startDate = ZonedDateTime.of(LocalDate.of(2024, 1, 1), LocalTime.MIDNIGHT, ZoneId.systemDefault())
    val windowDays = configuration.config.entitiesUpdatedSinceWindowDays

    // Precompute the fixed [windowStart, windowEnd) date windows spanning epoch -> endDate. Windows are independent
    // of each other, so unlike the pagination within a window (which must be sequential, since each 'start' offset
    // depends on the previous call's 'hasNext'), separate windows can be queried concurrently.
    def windows(current: ZonedDateTime): LazyList[(ZonedDateTime, ZonedDateTime)] =
      if !current.isBefore(endDate) then LazyList.empty
      else {
        val windowEnd = if current.plusDays(windowDays).isBefore(endDate) then current.plusDays(windowDays) else endDate
        (current, windowEnd) #:: windows(windowEnd)
      }

    // Each window is bounded by [windowStart, windowEnd) so that 'entitiesUpdatedSince' isn't asked to search
    // across the whole date range (epoch to now) in one go, which was timing out. Within a window, pagination
    // continues (incrementing 'start') until the API reports there are no more entries.
    def fetchWindow(windowStart: ZonedDateTime, windowEnd: ZonedDateTime): Stream[IO, CoRow] =
      Stream
        .unfoldEval(Option(0)) {
          case None        => IO.none
          case Some(start) =>
            client.entitiesUpdatedSince(windowStart, start, potentialEndDate = Option(windowEnd)).flatMap { entitiesUpdated =>
              entitiesUpdated.entities
                .filter(e => e.entityType.contains(ContentObject) && !e.deleted)
                .groupBy(_.ref)
                .keys
                .toList
                .grouped(configuration.config.maxConcurrency)
                .toList
                .parFlatTraverse(builder.run)
                .map(rows => (rows, if entitiesUpdated.hasNext then Some(start + 1000) else None).some)
            }
        }
        .flatMap(Stream.emits)

    def getEntities: Stream[IO, CoRow] =
      Stream
        .emits(windows(startDate))
        .covary[IO]
        .map(fetchWindow.tupled)
        .parJoin(configuration.config.entitiesUpdatedSinceConcurrency)

    val ps = getEntities
      .chunkN(1000)
      .evalTap(database.writeToPreservicaCOsTable)
      .compile
      .drain

    database.deleteFromTables() >> IO.both(ocfl, ps) >> database.findAllMissingCOs().flatMap { result =>
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
