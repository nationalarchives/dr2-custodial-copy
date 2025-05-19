package uk.gov.nationalarchives.utils

import cats.effect.{Async, Sync}
import cats.syntax.all.*
import doobie.util.{Get, Put}
import io.circe.Decoder
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId}
import io.ocfl.api.{OcflConfig, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.{OcflStorage, OcflStorageBuilder}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.DASQSClient.MessageResponse

import java.net.URI
import java.nio.file.Paths
import java.time.Instant
import java.util.UUID
object Utils:

  given Get[UUID] = Get[String].map(UUID.fromString)

  given Put[UUID] = Put[String].contramap(_.toString)

  given Get[Instant] = Get[Long].map(Instant.ofEpochMilli)

  given Put[Instant] = Put[Long].contramap(_.toEpochMilli)

  final case class OcflFile(
      versionNum: Long,
      id: UUID,
      name: Option[String],
      fileId: UUID,
      zref: Option[String],
      path: Option[String],
      fileName: Option[String],
      ingestDateTime: Option[Instant],
      sourceId: Option[String],
      citation: Option[String],
      consignmentRef: Option[String]
  )

  extension (uuid: UUID) def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)

  def createOcflRepository(ocflRepoDir: String, ocflWorkDir: String): OcflRepository = {
    val repoDir = Paths.get(ocflRepoDir)
    val workDir = Paths.get(ocflWorkDir)
    val storage: OcflStorage = OcflStorageBuilder.builder().fileSystem(repoDir).build
    val ocflConfig: OcflConfig = new OcflConfig()
    ocflConfig.setDefaultDigestAlgorithm(DigestAlgorithm.fromOcflName("sha256"))
    new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(storage)
      .ocflConfig(ocflConfig)
      .prettyPrintJson()
      .workDir(workDir)
      .build()
  }

  def aggregateMessages[F[_]: Sync, T: Decoder](sqs: DASQSClient[F], sqsQueueUrl: String): F[List[MessageResponse[T]]] = {
    def collectMessages(messages: List[MessageResponse[T]]): F[List[MessageResponse[T]]] = {
      sqs
        .receiveMessages[T](sqsQueueUrl)
        .flatMap { newMessages =>
          val allMessages = newMessages ++ messages
          if newMessages.isEmpty || allMessages.size >= 50 then Sync[F].pure(allMessages) else collectMessages(allMessages)
        }
        .handleErrorWith { err =>
          logError(err) >> Sync[F].pure[List[MessageResponse[T]]](messages)
        }
    }

    collectMessages(Nil)
  }

  def sqsClient[F[_]: Async](proxyUrl: Option[URI]): DASQSClient[F] =
    proxyUrl
      .map(proxy => DASQSClient[F](proxy))
      .getOrElse(DASQSClient[F]())

  def logError[F[_]: Sync](err: Throwable): F[Unit] = for {
    logger <- Slf4jLogger.create[F]
    _ <- logger.error(err)("There has been an error")
  } yield ()
