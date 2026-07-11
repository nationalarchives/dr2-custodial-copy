package uk.gov.nationalarchives.confirmer

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.nio.netty.{NettyNioAsyncHttpClient, ProxyConfiguration}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.confirmer.Config
import uk.gov.nationalarchives.confirmer.Confirmer.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}
import uk.gov.nationalarchives.utils.Utils.*

import java.net.URI
import java.net.http.HttpClient
import scala.concurrent.duration.*
import scala.language.postfixOps

object Main extends IOApp {

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
      .credentialsProvider(DefaultCredentialsProvider.builder.build)
      .build()

    DADynamoDBClient[IO](dynamoDBClient)
  }

  // The confirmerStream loads config once per evaluation of Stream.eval and returns the inner stream produced by the selected Confirmer.
  private def confirmerStream: Stream[IO, Unit] =
    Stream.eval {
      for {
        config <- ConfigSource.default.loadF[IO, Config]()
        sqs = sqsClient[IO](config.proxyUrl.some)
        dynamo = dynamoClient(config.proxyUrl)
        ocfl = Ocfl(config)
        scoutAm = ScoutAM(config, ScoutAmHttpService(HttpClient.newHttpClient()))
      } yield runConfirmer(config, sqs, dynamo, ocfl, scoutAm)
    }.flatten

  def runConfirmer(
      config: Config,
      sqsClient: DASQSClient[IO],
      dynamoClient: DADynamoDBClient[IO],
      ocfl: Ocfl,
      scoutAM: ScoutAM
  ): Stream[IO, Unit] =
    Stream
      .eval {
        for {
          logger <- Slf4jLogger.create[IO]
          messages <- aggregateMessages[IO, OutputQueueMessage](sqsClient, config.sqsUrl)
          _ <- IO.whenA(messages.nonEmpty)(logger.info(s"Processing message refs ${messages.map(_.message.payload.toString).mkString(",")}"))
          _ <- messages.parTraverse { sqsMessage =>
            val message = sqsMessage.message
            val payload = message.payload
            val confirmationService = ConfirmationService.getInstance(config, ocfl, scoutAM)
            val result: Result = Confirmer.getConfirmer(config).getResult(payload, confirmationService)

            result match
              case Result.Success(dynamoMap) =>

                val resultJson = Json.obj(dynamoMap.map { case (k, v) =>
                  k -> v.asJson
                }.toSeq*)
                val attributeUpdateMap =
                  Map(
                    config.dynamoAttributeName -> resultJson.noSpaces.toAttributeValue
                  )

                val request = DADynamoDbRequest(
                  config.dynamoTableName,
                  message.primaryKey,
                  attributeUpdateMap,
                  Option("attribute_exists(assetId)")
                )

                dynamoClient
                  .updateAttributeValues(request)
                  .handleErrorWith {
                    case _: ConditionalCheckFailedException => IO.unit
                    case e                                  => IO.raiseError(e)
                  } >>
                  sqsClient.deleteMessage(config.sqsUrl, sqsMessage.receiptHandle).void

              case Result.Failure(err) => logError[IO](err)
          }
        } yield ()
      }
      .handleErrorWith(err => Stream.eval(logError(err)))

  override def run(args: List[String]): IO[ExitCode] =
    (Stream.fixedRateStartImmediately[IO](60.seconds) >> confirmerStream).compile.drain
      .as(ExitCode.Success)
}
