package uk.gov.nationalarchives.builder

import cats.effect.Async
import cats.implicits.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.builder.Main.Message
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.DASQSClient.MessageResponse

import java.util.UUID

trait Builder[F[_]]:
  def run(messages: List[MessageResponse[Message]]): F[Unit]

object Builder:
  def apply[F[_]: Async](using ev: Builder[F]): Builder[F] = ev
  given impl[F[_]: Async: Ocfl: Database](using configuration: Configuration): Builder[F] = messages =>
    for {
      logger <- Slf4jLogger.create[F]
      _ <- logger.info(s"Received messages : ${messages.map(_.message.id.toString).mkString(",") } messages from queue")
      files <- messages.map(_.message.id).map(Ocfl[F].generateOcflObjects).sequence
      _ <- Database[F].write(files.flatten)
    } yield ()
