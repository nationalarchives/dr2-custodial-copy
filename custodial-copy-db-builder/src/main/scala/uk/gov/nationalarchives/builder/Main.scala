package uk.gov.nationalarchives.builder

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.*
import uk.gov.nationalarchives.builder.Configuration.impl
import fs2.Stream
import io.circe.{Decoder, HCursor}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigReader
import uk.gov.nationalarchives.DASQSClient

import java.net.URI
import java.util.UUID
import scala.concurrent.duration.*

object Main extends IOApp {

  case class Config(databasePath: String, queueUrl: String, ocflRepoDir: String, ocflWorkDir: String, proxyUrl: Option[URI] = None) derives ConfigReader

  case class Message(id: UUID)

  given Decoder[Message] = (c: HCursor) =>
    for {
      id <- c.downField("ioRef").as[String]
    } yield Message(UUID.fromString(id))

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running custodial copy")
  } yield ()

  override def run(args: List[String]): IO[ExitCode] = for {
    sqs <- IO(Configuration[IO].config.proxyUrl.map(url => DASQSClient[IO](url)).getOrElse(DASQSClient[IO]()))
    _ <- {
      Stream.fixedRateStartImmediately[IO](20.seconds) >>
        runBuilder(sqs)
          .handleErrorWith(err => Stream.eval(logError(err)))
    }.compile.drain
  } yield ExitCode.Success

  def runBuilder(sqs: DASQSClient[IO])(using configuration: Configuration): Stream[IO, Unit] = {
    val queueUrl = configuration.config.queueUrl
    Stream
      .eval(sqs.receiveMessages[Message](queueUrl))
      .evalMap { messages =>
        for {
          logger <- Slf4jLogger.create[IO]
          _ <- IO.whenA(messages.nonEmpty) {
            logger.info(s"Received ${messages.length} messages from queue $queueUrl") >> Builder[IO].run(messages)
          }
          _ <- messages.map(message => sqs.deleteMessage(queueUrl, message.receiptHandle)).sequence
        } yield ()
      }
  }

}
