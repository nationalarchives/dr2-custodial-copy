package uk.gov.nationalarchives.confirmer

import java.net.http.HttpRequest
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import io.circe.parser.parse
import io.circe.Json

import java.nio.file.Path

trait ScoutAM(config: TCConfig, httpService: ScoutAmHttpService):
  def getFileDetails(filePaths: List[String]): Map[String, List[String]]

object ScoutAM:
  def apply(config: TCConfig, httpService: ScoutAmHttpService): ScoutAM = new ScoutAM(config, httpService):

    private def getFileDetailsForPath(scoutAmBaseUrl: String, filePath: String, authorisationResponse: AuthorisationResponse): Either[Throwable, FileResponse] =
      val fullFilePath = Path.of(config.mountRoot, filePath).toString
      val encodedFilePath = URLEncoder.encode(fullFilePath, StandardCharsets.UTF_8)
      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(s"$scoutAmBaseUrl/v1/file?path=$encodedFilePath"))
        .header("Authorization", s"Bearer ${authorisationResponse.token}")
        .header("Accept", "application/json")
        .GET()
        .build()
      httpService.get(request) match
        case response if response.statusCode() == 200 =>
          val jsonResponse = parse(response.body()).getOrElse(Json.Null)
          jsonResponse.as[FileResponse]
        case response =>
          Left(new RuntimeException(s"Failed to retrieve file details for $filePath with status code: ${response.statusCode()}"))

    override def getFileDetails(filePaths: List[String]): Map[String, List[String]] =
      val baseUrl = config.scoutamBaseUrl
      val username = config.scoutamUsername
      val password = config.scoutamPassword
      val authorisationResponse = authenticate(baseUrl, username, password)

      val results = filePaths.map(eachFilePath => eachFilePath -> getFileDetailsForPath(baseUrl, eachFilePath, authorisationResponse)).toMap
      results.flatMap { case (filePath, result) =>
        for {
          fileResponse <- result.toOption
          if fileResponse.archdone
          copy3 <- fileResponse.copies.find(_.copy == "3")
          if copy3.sections.isDefined && copy3.sections.get.nonEmpty
        } yield filePath -> copy3.sections.get.map(_.volume)
      }

    private def authenticate(baseUrl: String, username: String, password: String): AuthorisationResponse =

      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(s"$baseUrl/v1/security/login"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(s"""{"acct":"$username","pass":"$password"}"""))
        .build()
      httpService.post(request) match
        case response if response.statusCode() == 200 =>
          val jsonResponse = parse(response.body()).getOrElse(Json.Null)
          jsonResponse.as[AuthorisationResponse] match {
            case Right(authResponse) => authResponse
            case Left(error)         => throw new RuntimeException(s"Failed to parse authentication response: $error")
          }
        case response =>
          throw new RuntimeException(s"Authentication failed with status code: ${response.statusCode()}")
