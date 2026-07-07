package uk.gov.nationalarchives.confirmer

import org.scalatest.matchers.should.Matchers.*

import java.net.http.{HttpRequest, HttpResponse}

class ScoutAMTest extends org.scalatest.flatspec.AnyFlatSpec {
  "Get file details" should "return the volume details when they are available" in {
    val responseForFileDetails =
      """
        |{
        |   "archdone":true, 
        |   "copies":[
        |      {"copy": "1"},
        |      {"copy": "2"},
        |      {"copy": "3", "sections": [{"volume": "Volume1234"}, {"volume": "Volume5678"}]}
        |   ],
        |   "checksum": "someChecksumValue"
        |}""".stripMargin
    val scoutAM = ScoutAM(
      Config("table", "TC_result", "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService(200, 200, responseForFileDetails)
    )
    val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    details should contain allOf ("/tmp/file1" -> List("Volume1234", "Volume5678"), "/tmp/file2" -> List("Volume1234", "Volume5678"))
  }

  "Get file details" should "return an empty map when archdone is false" in {
    val responseForFileDetails =
      """
        |{
        |   "archdone":false, 
        |   "copies":[
        |      {"copy": "1"},
        |      {"copy": "2"},
        |      {"copy": "3", "sections": [{"volume": "Volume1234"}, {"volume": "Volume5678"}]}
        |   ],
        |   "checksum": "someChecksumValue"
        |}""".stripMargin
    val scoutAM = ScoutAM(
      Config("table", "TC_result", "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService(200, 200, responseForFileDetails)
    )
    val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    details shouldBe empty
  }

  "Authenticate" should "error when authentication is unsuccessful" in {
    val scoutAM = ScoutAM(
      Config("table", "TC_result", "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService(401, 200, """{"status": "does-not-matter"}""")
    )
    val ex = intercept[Exception] {
      val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    }
    ex.getMessage should be("Authentication failed with status code: 401")
  }

  "Get file details" should "return an empty map when unable to retrieve file details" in {
    val scoutAM = ScoutAM(
      Config("table", "TC_result", "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService(200, 401, """{"error":"Unauthorized"}""")
    )
    val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    details shouldBe empty
  }

  "Get file details" should "return an empty map even if archdone is true but there are no 3 copies in the response" in {
    val responseForFileDetails =
      """
        |{
        |  "archdone": true,
        |  "copies": [
        |    {"copy": "1"},
        |    {"copy": "2"}
        |  ],
        |  "checksum": null
        |}""".stripMargin
    val scoutAM = ScoutAM(
      Config("table", "TC_result", "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService(200, 200, responseForFileDetails)
    )
    val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    details shouldBe empty
  }

  "Get file details" should "return an empty map when archdone is true and there are 3 copies but no volume information" in {
    val responseForFileDetails =
      """
        |{
        |  "archdone": true,
        |  "copies": [
        |    {"copy": "1"},
        |    {"copy": "2"},
        |    {"copy": "3"}
        |  ],
        |  "checksum": null
        |}""".stripMargin
    val scoutAM = ScoutAM(
      Config("table", "TC_result", "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService(200, 200, responseForFileDetails)
    )
    val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    details shouldBe empty
  }
}

class TestHttpService(authStatus: Int = 200, fileStatus: Int = 200, successfulFileResponse: String) extends ScoutAmHttpService {
  override def get(request: HttpRequest): HttpResponse[String] = {
    request.uri().toString match {
      case uri if uri.contains("/v1/file") =>
        val filename = uri.split("path=").last.split("%2F").last
        if fileStatus == 200 then {
          if successfulFileResponse.nonEmpty then new TestHttpResponse(200, successfulFileResponse)
          else
            new TestHttpResponse(
              200,
              s"""{"archdone":true, "copies":[{"copy": "1"},{"copy": "2"},{"copy": "3", "sections": [{"volume": "Volume1-$filename"}, {"volume": "Volume2-$filename"}]}],"checksum": "someChecksumValue"}"""
            )
        } else {
          new TestHttpResponse(fileStatus, """{"error":"Unauthorized"}""")
        }
      case _ => throw new NotImplementedError("Only /v1/file endpoint is implemented in TestHttpService")
    }
  }

  override def post(request: HttpRequest): HttpResponse[String] = {
    request.uri().toString match {
      case uri if uri.contains("/v1/auth") =>
        if authStatus == 200 then new TestHttpResponse(200, """{"response": "valid-token"}""")
        else {
          new TestHttpResponse(authStatus, """{"error": "Unauthorized"}""")
        }
      case _ => throw new NotImplementedError("Only /v1/auth endpoint is implemented in TestHttpService")
    }
  }
}

class TestHttpResponse(status: Int, responseBody: String) extends HttpResponse[String] {
  override def statusCode(): Int = status
  override def body(): String = responseBody
  override def headers(): java.net.http.HttpHeaders = java.net.http.HttpHeaders.of(java.util.Map.of(), (s1, s2) => true)
  override def request(): HttpRequest = null
  override def previousResponse(): java.util.Optional[HttpResponse[String]] = java.util.Optional.empty()
  override def sslSession(): java.util.Optional[javax.net.ssl.SSLSession] = java.util.Optional.empty()
  override def uri(): java.net.URI = null
  override def version(): java.net.http.HttpClient.Version = throw new NotImplementedError("Not implemented")
}
