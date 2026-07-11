package uk.gov.nationalarchives.confirmer

import uk.gov.nationalarchives.confirmer.ResultAttributeName.TC_RESULT
import org.scalatest.matchers.should.Matchers.*

import java.net.http.{HttpRequest, HttpResponse}

class ScoutAMTest extends org.scalatest.flatspec.AnyFlatSpec {
  "ScoutAM" should "return a valid instance of ScoutAM based on the config" in {
    val scoutAM = ScoutAM(
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService()
    )
    scoutAM shouldBe a[ScoutAM]
  }

  "ScoutAM" should "throw an exception when the config is missing required parameters" in {
    val ex = intercept[Exception] {
      ScoutAM(
        Config("table", TC_RESULT.toString, "", null, "", "", None, Some("scout.username"), Some("scout.password")),
        new TestHttpService()
      )
    }
    ex.getMessage should equal("ScoutAM base URL is not configured")
  }

  "ScoutAM" should "throw an exception when the config is missing username or password" in {
    val ex1 = intercept[Exception] {
      ScoutAM(
        Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), None, Some("scout.password")),
        new TestHttpService()
      )
    }
    ex1.getMessage should equal("Unable to authenticate, ScoutAM credentials not found")

    val ex2 = intercept[Exception] {
      ScoutAM(
        Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), None),
        new TestHttpService()
      )
    }
    ex2.getMessage should equal("Unable to authenticate, ScoutAM credentials not found")
  }

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
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService("", responseForFileDetails, 200, 200)
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
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService("", responseForFileDetails, 200, 200)
    )
    val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    details shouldBe empty
  }

  "Authenticate" should "error when authentication is unsuccessful" in {
    val scoutAM = ScoutAM(
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService("", """{"status": "does-not-matter"}""", 401, 200)
    )
    val ex = intercept[Exception] {
      val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    }
    ex.getMessage should be("Authentication failed with status code: 401")
  }

  "Authenticate" should "error when authentication response cannot be parsed" in {
    val scoutAM = ScoutAM(
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService("""{"this": "cannot-be-parsed"}""", "", 200, 200)
    )
    val ex = intercept[Exception] {
      val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    }
    ex.getMessage should include("Failed to parse authentication response")
  }

  "Get file details" should "return an empty map when unable to retrieve file details" in {
    val scoutAM = ScoutAM(
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService("", """{"error":"Unauthorized"}""", 200, 401)
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
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService("", responseForFileDetails, 200, 200)
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
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      new TestHttpService("", responseForFileDetails, 200, 200)
    )
    val details = scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
    details shouldBe empty
  }
}

class TestHttpService(authResponse: String = "", fileResponse: String = "", authStatus: Int = 200, fileStatus: Int = 200) extends ScoutAmHttpService {
  override def get(request: HttpRequest): HttpResponse[String] = {
    request.uri().toString match {
      case uri if uri.contains("/v1/file") =>
        val filename = uri.split("path=").last.split("%2F").last
        if fileStatus == 200 then
          if fileResponse.nonEmpty then new TestHttpResponse(fileStatus, fileResponse)
          else
            new TestHttpResponse(
              200,
              s"""{"archdone":true, "copies":[{"copy": "1"},{"copy": "2"},{"copy": "3", "sections": [{"volume": "Volume1-$filename"}, {"volume": "Volume2-$filename"}]}],"checksum": "someChecksumValue"}"""
            )
        else new TestHttpResponse(fileStatus, """{"error":"Unauthorized"}""")
      case _ => throw new NotImplementedError("Only /v1/file endpoint is implemented in TestHttpService")
    }
  }

  override def post(request: HttpRequest): HttpResponse[String] = {
    request.uri().toString match {
      case uri if uri.contains("/v1/auth") =>
        if authStatus == 200 then
          if authResponse.nonEmpty then new TestHttpResponse(authStatus, authResponse)
          else new TestHttpResponse(200, """{"response": "valid-token"}""")
        else new TestHttpResponse(authStatus, """{"error": "Unauthorized"}""")
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
