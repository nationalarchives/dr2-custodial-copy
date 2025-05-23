package uk.gov.nationalarchives.confirmer

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import fs2.Stream
import pureconfig.ConfigSource
import uk.gov.nationalarchives.utils.Utils.*
import pureconfig.*
import pureconfig.module.catseffect.syntax.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}
import io.circe.generic.auto.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.nio.netty.{NettyNioAsyncHttpClient, ProxyConfiguration}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.net.URI
import scala.concurrent.duration.*
import java.util.UUID

object Main extends IOApp {

  case class Config(
      dynamoTableName: String,
      dynamoAttributeName: String,
      sqsUrl: String,
      proxyUrl: URI,
      ocflRepoDir: String,
      ocflWorkDir: String,
      preservicaUrl: String,
      preservicaSecretName: String
  ) derives ConfigReader

  extension (s: String) private def toAttributeValue: AttributeValue = AttributeValue.builder.s(s).build

  case class Message(assetId: UUID, batchId: String) {
    def primaryKey: Map[String, AttributeValue] =
      Map("assetId" -> assetId.toString.toAttributeValue, "batchId" -> batchId.toAttributeValue)
  }

  private def dynamoClient(proxyUrl: URI): DADynamoDBClient[IO] = {
    val proxy = ProxyConfiguration
      .builder()
      .scheme(proxyUrl.getScheme)
      .host(proxyUrl.getHost)
      .port(proxyUrl.getPort)
      .build
    val dynamoDBClient: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .httpClient(NettyNioAsyncHttpClient.builder().proxyConfiguration(proxy).build())
      .region(Region.EU_WEST_2)
      .credentialsProvider(DefaultCredentialsProvider.create())
      .build()

    DADynamoDBClient[IO](dynamoDBClient)
  }

  override def run(args: List[String]): IO[ExitCode] =
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      sqs = sqsClient[IO](config.proxyUrl.some)
      dynamo = dynamoClient(config.proxyUrl)
      preservicaClient <- Fs2Client.entityClient(config.preservicaUrl, config.preservicaSecretName, potentialProxyUrl = config.proxyUrl.some)
      _ <- (Stream.fixedRateStartImmediately[IO](10.seconds) >> runConfirmer(config, sqs, dynamo, Ocfl(config), preservicaClient)).compile.drain
    } yield ExitCode.Success

  def runConfirmer(
      config: Config,
      sqsClient: DASQSClient[IO],
      dynamoClient: DADynamoDBClient[IO],
      ocfl: Ocfl,
      entityClient: EntityClient[IO, Fs2Streams[IO]]
  ): Stream[IO, Unit] = Stream
    .eval {
      for {
        logger <- Slf4jLogger.create[IO]
        messages <- aggregateMessages[IO, Message](sqsClient, config.sqsUrl)
        _ <- logger.info(s"Processing message refs ${messages.map(_.message.assetId).mkString(",")}")
        _ <- messages.parTraverse { sqsMessage =>
          val message = sqsMessage.message
          val request = DADynamoDbRequest(config.dynamoTableName, message.primaryKey, Map(config.dynamoAttributeName -> "true".toAttributeValue.some))
          val identifier = Identifier("SourceId", message.assetId.toString)
          for {
            entities <- entityClient.entitiesPerIdentifier(Seq(identifier))
            _ <- IO.raiseWhen(entities(identifier).size != 1)(
              new RuntimeException(s"Expected 1 entity for SourceID ${message.assetId} but found ${entities.get(identifier).size}")
            )
            _ <- IO.whenA(ocfl.checkObjectExists(entities(identifier).head.ref)) {
              dynamoClient.updateAttributeValues(request) >> sqsClient.deleteMessage(config.sqsUrl, sqsMessage.receiptHandle).void
            }
          } yield ()

        }
      } yield ()
    }
    .handleErrorWith(err => Stream.eval(logError(err)))

}
