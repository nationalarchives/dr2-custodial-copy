package uk.gov.nationalarchives.builder

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.*
import uk.gov.nationalarchives.builder.Configuration.impl
import fs2.Stream
import io.circe.generic.auto.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import uk.gov.nationalarchives.DASQSClient

import java.util.UUID
import scala.concurrent.duration.*

object Main extends IOApp {

  case class Config(databasePath: String, queueUrl: String, ocflRepoDir: String, ocflWorkDir: String) derives ConfigReader

  case class Message(id: UUID)

  private def logError(err: Throwable) = for {
    logger <- Slf4jLogger.create[IO]
    _ <- logger.error(err)("Error running disaster recovery")
  } yield ()

  override def run(args: List[String]): IO[ExitCode] = for {
    sqs <- IO(DASQSClient[IO]())
    _ <- {
      Stream.fixedRateStartImmediately[IO](20.seconds) >>
        runBuilder(sqs)
          .handleErrorWith(err => Stream.eval(logError(err)))
    }.compile.drain
  } yield ExitCode.Success

  def runBuilder(sqs: DASQSClient[IO])(using configuration: Configuration): Stream[IO, Unit] = {
    val queueUrl = configuration.config.queueUrl
    Stream
      .eval {
        for {
          messages <- sqs.receiveMessages[Message](queueUrl)
        } yield messages
      }
      .evalMap { messages =>
        for {
          _ <- IO.whenA(messages.nonEmpty)(Builder[IO].run(messages))
          _ <- messages.map(message => sqs.deleteMessage(queueUrl, message.receiptHandle)).sequence
        } yield ()
      }
  }

}
