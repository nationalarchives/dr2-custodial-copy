package uk.gov.nationalarchives.reconciler

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.module.catseffect.syntax.*
import pureconfig.*
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DAEventBridgeClient
import uk.gov.nationalarchives.dp.client.Entities.EntityRef.{ContentObjectRef, InformationObjectRef}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.reconciler.Configuration.impl
import uk.gov.nationalarchives.reconciler.OcflService
import uk.gov.nationalarchives.utils.DetailType.DR2DevMessage
import uk.gov.nationalarchives.utils.{Detail, OcflServiceConfig}

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
  ) extends OcflServiceConfig derives ConfigReader
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
      ocflService <- OcflService(config)
      eventBridgeClient = DAEventBridgeClient[IO]()
      _ <- IO(runReconciler(client, ocflService, eventBridgeClient).handleErrorWith(err => Stream.eval(logError(err))))
    } yield ExitCode.Success

  def runReconciler(client: EntityClient[IO, Fs2Streams[IO]], ocflService: OcflService, eventBridgeClient: DAEventBridgeClient[IO])(using
      configuration: Configuration
  ): Stream[IO, Unit] = {
    val database = Database[IO]

    client
      .streamAllEntityRefs()
      .filter {
        case _: InformationObjectRef | _: ContentObjectRef => true
        case _                                             => false
      }
      .asInstanceOf[Stream[IO, InformationObjectRef | ContentObjectRef]]
      .chunkN(configuration.config.maxConcurrency)
      .parEvalMap(configuration.config.maxConcurrency) { entityRefChunks =>
        for {
          coRowChunks <- Builder[IO].run(client, ocflService, entityRefChunks)
        } yield Stream.chunk(coRowChunks)
      }
      .parJoin(configuration.config.maxConcurrency)
      .evalTap { coRows =>
        database.writeToActuallyInPsTable(coRows.listOfPreservicaCoRows) >>
          database.writeToExpectedInPsTable(coRows.listOfOcflCoRows)
      }
      .evalMap { coRows =>
        for {
          psCosMissingInCc <- coRows.listOfPreservicaCoRows.flatTraverse(database.checkIfPsCoInCc)
          _ <- IO.whenA(psCosMissingInCc.nonEmpty) { sendCosToSlack(psCosMissingInCc, eventBridgeClient).void }
          ccCosMissingInPs <- coRows.listOfOcflCoRows.flatTraverse(database.checkIfCcCoInPs)
          _ <- IO.whenA(ccCosMissingInPs.nonEmpty) { sendCosToSlack(ccCosMissingInPs, eventBridgeClient).void }
        } yield ()
      }
  }

  private def sendCosToSlack(missingCoMessages: List[String], eventBridgeClient: DAEventBridgeClient[IO]): IO[PutEventsResponse] = {
    val combinedMessages = missingCoMessages.mkString("\n")
    eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2DevMessage, Detail(combinedMessages))
  }
}
