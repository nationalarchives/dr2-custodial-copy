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
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest

import java.net.URI
import scala.concurrent.duration.*
import java.util.UUID

object Main extends IOApp {

  case class Config(dynamoTableName: String, dynamoAttributeName: String, sqsUrl: String, proxyUrl: URI, ocflRepoDir: String, ocflWorkDir: String)
      derives ConfigReader

  extension (s: String) private def toAttributeValue: AttributeValue = AttributeValue.builder.s(s).build

  case class Message(ioRef: UUID, batchId: String) {
    def primaryKey: Map[String, AttributeValue] =
      Map("ioRef" -> ioRef.toString.toAttributeValue, "batchId" -> batchId.toAttributeValue)
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
      _ <- (Stream.fixedRateStartImmediately[IO](10.seconds) >> runConfirmer(config, sqs, dynamo, Ocfl(config))).compile.drain
    } yield ExitCode.Success

  def runConfirmer(config: Config, sqsClient: DASQSClient[IO], dynamoClient: DADynamoDBClient[IO], ocfl: Ocfl): Stream[IO, Unit] = Stream
    .eval {
      for {
        logger <- Slf4jLogger.create[IO]
        messages <- aggregateMessages[IO, Message](sqsClient, config.sqsUrl)
        _ <- logger.info(s"Processing message refs ${messages.map(_.message.ioRef).mkString(",")}")
        _ <- messages.parTraverse { sqsMessage =>
          val message = sqsMessage.message
          val request = DADynamoDbRequest(config.dynamoTableName, message.primaryKey, Map(config.dynamoAttributeName -> "true".toAttributeValue.some))
          IO.whenA(ocfl.checkObjectExists(message.ioRef)) {
            dynamoClient.updateAttributeValues(request) >> sqsClient.deleteMessage(config.sqsUrl, sqsMessage.receiptHandle).void
          }

        }
      } yield ()
    }
    .handleErrorWith(err => Stream.eval(logError(err)))
}
