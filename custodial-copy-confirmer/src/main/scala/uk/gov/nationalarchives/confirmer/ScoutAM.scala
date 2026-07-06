package uk.gov.nationalarchives.confirmer

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import io.circe.parser.parse
import io.circe.Json
import io.circe.Decoder

final case class AuthorisationResponse(token: String)

object AuthorisationResponse:
  given Decoder[AuthorisationResponse] = Decoder.instance { c =>
    for {
      token <- c.get[String]("token")
    } yield AuthorisationResponse(token)
  }

trait ScoutAM(config: Config):
  def getFileDetails(filePaths: List[String]): Map[String, List[String]]

object ScoutAM:
  def apply(config: Config): ScoutAM = new ScoutAM(config):
    def getFileDetailsForPath(filePath: String, authorisationResponse: AuthorisationResponse): Either[Throwable, FileResponse] =
      val encodedFilePath = URLEncoder.encode(filePath, StandardCharsets.UTF_8.toString)
      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(s"${config.scoutAmBaseUrl}/v1/file?path=$encodedFilePath"))
        .header("Authorization", s"Bearer ${authorisationResponse.token}")
        .header("Accept", "application/json")
        .GET()
        .build()
      val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
      response.statusCode() match {
        case 200 =>
          val jsonResponse = parse(response.body()).getOrElse(Json.Null)
          jsonResponse.as[FileResponse] match {
            case Right(fileResponse) => Right(fileResponse)
            case Left(error)         => Left(error)
          }
        case anyOtherStatusCode =>
          Left(new RuntimeException(s"Failed to retrieve file details with status code: $anyOtherStatusCode"))
      }

    override def getFileDetails(filePaths: List[String]): Map[String, List[String]] = {
      val authorisationResponse = authenticate(
        config.scoutAmUsername.getOrElse(throw new RuntimeException("Unable to authenticate, ScoutAM credentials not found")),
        config.scoutAmPassword.getOrElse(throw new RuntimeException("Unable to authenticate, ScoutAM credentials not found"))
      )

      val results = filePaths.map(eachFilePath => eachFilePath -> getFileDetailsForPath(eachFilePath, authorisationResponse)).toMap
      results.flatMap { case (filePath, result) =>
        for {
          fileResponse <- result.toOption
          copy3 <- fileResponse.copies.find(_.copy == "3")
          if copy3.sections.isDefined && copy3.sections.get.nonEmpty
        } yield filePath -> copy3.sections.get.map(_.volume)
      }
    }

    def authenticate(username: String, password: String): AuthorisationResponse = {
      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(s"${config.scoutAmBaseUrl}/v1/auth"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(s"""{"username":"$username","password":"$password"}"""))
        .build()
      val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
      response.statusCode() match {
        case 200 =>
          val jsonResponse = parse(response.body()).getOrElse(Json.Null)
          jsonResponse.as[AuthorisationResponse] match {
            case Right(authResponse) => authResponse
            case Left(error)         => throw new RuntimeException(s"Failed to parse authentication response: $error")
          }
        case _ =>
          throw new RuntimeException(s"Authentication failed with status code: ${response.statusCode()}")
      }
    }
