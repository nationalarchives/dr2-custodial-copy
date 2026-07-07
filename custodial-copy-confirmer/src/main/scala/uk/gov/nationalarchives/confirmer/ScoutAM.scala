package uk.gov.nationalarchives.confirmer

import java.net.http.HttpRequest
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import io.circe.parser.parse
import io.circe.Json

trait ScoutAM(config: Config, httpService: ScoutAmHttpService):
  def getFileDetails(filePaths: List[String]): Map[String, List[String]]

object ScoutAM:
  def apply(config: Config, httpService: ScoutAmHttpService): ScoutAM = new ScoutAM(config, httpService):
    def getFileDetailsForPath(filePath: String, authorisationResponse: AuthorisationResponse): Either[Throwable, FileResponse] =
      val scoutAmBaseUrl = config.scoutAmBaseUrl.getOrElse(throw new RuntimeException("ScoutAM base URL is not configured"))
      val encodedFilePath = URLEncoder.encode(filePath, StandardCharsets.UTF_8.toString)
      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(s"$scoutAmBaseUrl/v1/file?path=$encodedFilePath"))
        .header("Authorization", s"Bearer ${authorisationResponse.token}")
        .header("Accept", "application/json")
        .GET()
        .build()
      httpService.get(request) match {
        case response if response.statusCode() == 200 =>
          val jsonResponse = parse(response.body()).getOrElse(Json.Null)
          jsonResponse.as[FileResponse] match {
            case Right(fileResponse) => Right(fileResponse)
            case Left(error)         => Left(error)
          }
        case response =>
          Left(new RuntimeException(s"Failed to retrieve file details for $filePath with status code: ${response.statusCode()}"))
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
          if fileResponse.archdone
          copy3 <- fileResponse.copies.find(_.copy == "3")
          if copy3.sections.isDefined && copy3.sections.get.nonEmpty
        } yield filePath -> copy3.sections.get.map(_.volume)
      }
    }

    def authenticate(username: String, password: String): AuthorisationResponse = {
      val scoutAmBaseUrl = config.scoutAmBaseUrl.getOrElse(throw new RuntimeException("ScoutAM base URL is not configured"))
      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(s"$scoutAmBaseUrl/v1/auth"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(s"""{"username":"$username","password":"$password"}"""))
        .build()
      httpService.post(request) match {
        case response if response.statusCode() == 200 =>
          val jsonResponse = parse(response.body()).getOrElse(Json.Null)
          jsonResponse.as[AuthorisationResponse] match {
            case Right(authResponse) => authResponse
            case Left(error)         => throw new RuntimeException(s"Failed to parse authentication response: $error")
          }
        case response =>
          throw new RuntimeException(s"Authentication failed with status code: ${response.statusCode()}")
      }
    }
