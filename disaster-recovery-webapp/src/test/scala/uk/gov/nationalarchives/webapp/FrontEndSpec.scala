package uk.gov.nationalarchives.webapp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import org.http4s.*
import org.http4s.implicits.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.utils.Utils
import uk.gov.nationalarchives.utils.Utils.OcflFile

import java.nio.file.Files
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

class FrontEndSpec extends AnyFlatSpec:

  private val ingestDateTime: Instant = LocalDate.of(2024, 7, 9).atStartOfDay(ZoneOffset.UTC).toInstant

  private def assertContains(uri: String, text: String) =
    ocflRoutes(uri).flatMap(_.as[String].map(_.contains(text))).unsafeRunSync() should equal(true)

  private def ocflRoutes(url: String)(using Assets[IO]): IO[Response[IO]] =
    val request = Request[IO](Method.GET, Uri.unsafeFromString(url))
    FrontEndRoutes.ocflRoutes[IO].orNotFound(request)

  "OcflRoutes /" should "return status code 200" in {
    ocflRoutes("/").map(_.status).unsafeRunSync() should equal(Status.Ok)
  }

  "OcflRoutes /" should "return search form" in {
    assertContains("/", "<h1 class=\"govuk-heading-l\">Search for a file</h1>")
    assertContains("/", "<input class=\"govuk-input\" id=\"id\" name=\"id\" type=\"text\"/>")
    assertContains("/", "<input class=\"govuk-input\" id=\"zref\" name=\"zref\" type=\"text\"/>")
  }

  "OcflRoutes /download" should "download a file for a given id" in {
    val id = UUID.randomUUID()
    val file = Files.createTempFile("prefix", "suffix")

    given Assets[IO] = new Assets[IO]:
      override def filePath(id: UUID): IO[String] = IO {
        Files.write(file, id.toString.getBytes)
        file.toString
      }

      override def findFiles(searchResponse: FrontEndRoutes.SearchResponse): IO[List[Utils.OcflFile]] = IO(Nil)

    ocflRoutes(s"/download/$id").flatMap(_.as[Array[Byte]].map(_.sameElements(id.toString.getBytes))).unsafeRunSync() should equal(true)
  }

  "OcflRoutes /search GET" should "render search results if results are found" in {
    val id = UUID.fromString("a73bce36-02e3-4d7a-b3ea-4e2bdd1fa46d")
    given Assets[IO] = new Assets[IO]:
      override def filePath(id: UUID): IO[String] = IO("")
      override def findFiles(searchResponse: FrontEndRoutes.SearchResponse): IO[List[Utils.OcflFile]] =
        IO(
          List(
            OcflFile(
              1,
              UUID.randomUUID,
              "name".some,
              searchResponse.id.get,
              "zref".some,
              "path".some,
              searchResponse.zref,
              ingestDateTime.some,
              "sourceId".some,
              "citation".some
            )
          )
        )

    val request = Request[IO](Method.GET, uri"/search?id=a73bce36-02e3-4d7a-b3ea-4e2bdd1fa46d&zref=zref")
    val response = FrontEndRoutes.ocflRoutes[IO].orNotFound(request)
    def searchContains(text: String) = response.flatMap(_.as[String].map(_.contains(text))).unsafeRunSync() should equal(true)

    searchContains("<caption class=\"govuk-table__caption govuk-table__caption--m\">Search Results</caption>")
    searchContains(s"<a class=\"govuk-link\" href=\"/download/$id\" download=\"zref\">Download</a>")
    searchContains("""<td class="govuk-table__cell">zref</td>""")
    searchContains("""<td class="govuk-table__cell govuk-!-width-one-quarter">2024-07-09</td>""")
    searchContains("""<td class="govuk-table__cell">citation</td>""")
    searchContains("<th scope=\"row\" class=\"govuk-table__header\">zref</th>")
  }

  "OcflRoutes /search GET" should "pass the correct parameters to the database call" in {
    val id = UUID.randomUUID.toString

    given Assets[IO] = new Assets[IO]:
      override def filePath(id: UUID): IO[String] = IO("")

      override def findFiles(searchResponse: FrontEndRoutes.SearchResponse): IO[List[Utils.OcflFile]] = {
        assert(searchResponse.id.get.toString == "a73bce36-02e3-4d7a-b3ea-4e2bdd1fa46d")
        assert(searchResponse.zref.get == "zref")
        assert(searchResponse.citation.get == "citation")
        assert(searchResponse.sourceId.get == "sourceId")
        assert(searchResponse.ingestDateTime.get == ingestDateTime)
        IO(
          List(
            OcflFile(
              1,
              UUID.randomUUID,
              "name".some,
              searchResponse.id.get,
              "zref".some,
              "path".some,
              searchResponse.zref,
              ingestDateTime.some,
              "sourceId".some,
              "citation".some
            )
          )
        )
      }

    val request =
      Request[IO](Method.GET, uri"/search?id=a73bce36-02e3-4d7a-b3ea-4e2bdd1fa46d&zref=zref&citation=citation&sourceId=sourceId&ingestDate=1720483200000")
    val response = FrontEndRoutes.ocflRoutes[IO].orNotFound(request).unsafeRunSync()
  }

  "OcflRoutes /search GET" should "not pass parameters to the database call if they are missing from the request" in {
    val id = UUID.randomUUID.toString

    given Assets[IO] = new Assets[IO]:
      override def filePath(id: UUID): IO[String] = IO("")

      override def findFiles(searchResponse: FrontEndRoutes.SearchResponse): IO[List[Utils.OcflFile]] = {
        assert(searchResponse.id.isEmpty)
        assert(searchResponse.zref.get == "zref")
        assert(searchResponse.citation.get == "citation")
        assert(searchResponse.sourceId.isEmpty)
        assert(searchResponse.ingestDateTime.isEmpty)
        IO(
          List(
            OcflFile(
              1,
              UUID.randomUUID,
              "name".some,
              UUID.randomUUID,
              "zref".some,
              "path".some,
              "fileName".some,
              ingestDateTime.some,
              "sourceId".some,
              "citation".some
            )
          )
        )
      }

    val request = Request[IO](Method.GET, uri"/search?zref=zref&citation=citation")
    val response = FrontEndRoutes.ocflRoutes[IO].orNotFound(request).unsafeRunSync()
  }

  "OcflRoutes /search GET" should "render no results if no results are found" in {
    val id = UUID.randomUUID.toString

    given Assets[IO] = new Assets[IO]:
      override def filePath(id: UUID): IO[String] = IO("")

      override def findFiles(searchResponse: FrontEndRoutes.SearchResponse): IO[List[Utils.OcflFile]] = IO(Nil)

    val request = Request[IO](Method.POST, uri"/search").withEntity(UrlForm(("id", id), ("zref", "missingZref")))
    val response = FrontEndRoutes.ocflRoutes[IO].orNotFound(request)

    def searchNotContains(text: String) = response.flatMap(_.as[String].map(page => !page.contains(text))).unsafeRunSync() should equal(true)

    searchNotContains(s"<a class=\"govuk-link\" href=\"/download/$id\" download=\"zref\">Download</a>")
    searchNotContains("<th scope=\"row\" class=\"govuk-table__header\">zref</th>")
  }

  "OcflRoutes /search POST" should "redirect to the correct url" in {
    val id = UUID.randomUUID.toString

    given Assets[IO] = new Assets[IO]:
      override def filePath(id: UUID): IO[String] = IO("")

      override def findFiles(searchResponse: FrontEndRoutes.SearchResponse): IO[List[Utils.OcflFile]] =
        IO(
          List(
            OcflFile(
              1,
              UUID.randomUUID,
              "name".some,
              searchResponse.id.get,
              "zref".some,
              "path".some,
              searchResponse.zref,
              ingestDateTime.some,
              "sourceId".some,
              "citation".some
            )
          )
        )

    val request = Request[IO](Method.POST, uri"/search").withEntity(UrlForm(("id", id), ("zref", "zref")))
    val response = FrontEndRoutes.ocflRoutes[IO].orNotFound(request).unsafeRunSync()
    assert(response.status == Status.Found)
    assert(response.headers.headers.head.value == s"search?id=$id&zref=zref")
  }
