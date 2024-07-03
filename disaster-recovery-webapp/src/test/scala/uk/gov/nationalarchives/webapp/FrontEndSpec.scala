package uk.gov.nationalarchives.webapp

import cats.effect.IO
import uk.gov.nationalarchives.utils.Utils.OcflFile
import org.http4s.*
import org.http4s.implicits.*
import org.scalatest.flatspec.AnyFlatSpec
import cats.effect.unsafe.implicits.global
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.utils.Utils

import java.nio.file.Files
import java.util.UUID

class FrontEndSpec extends AnyFlatSpec:

  private def assertContains(uri: String, text: String) = {
    ocflRoutes(uri).flatMap(_.as[String].map(_.contains(text))).unsafeRunSync() should equal(true)
  }

  private def ocflRoutes(url: String)(using Assets[IO]): IO[Response[IO]] =
    val request = Request[IO](Method.GET, Uri.unsafeFromString(url))
    FrontEndRoutes.ocflRoutes[IO].orNotFound(request)

  "OcflRoutes /" should "return status code 200" in {
    ocflRoutes("/").map(_.status).unsafeRunSync() should equal(Status.Ok)
  }

  "OcflRoutes /" should "return search form" in {
    assertContains("/", "<h1 class=\"govuk-heading-l\">Search for a file</h1>")
    assertContains("/", "<input  class=\"govuk-input\" id=\"id\" name=\"id\" type=\"text\"/>")
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

  "OcflRoutes /search" should "render search results if results are found" in {
    val id = UUID.randomUUID.toString
    given Assets[IO] = new Assets[IO]:
      override def filePath(id: UUID): IO[String] = IO("")
      override def findFiles(searchResponse: FrontEndRoutes.SearchResponse): IO[List[Utils.OcflFile]] =
        IO(List(OcflFile(1, "id", "name", searchResponse.id.get, "zref", "path", searchResponse.zref.get)))

    val request = Request[IO](Method.POST, uri"/search").withEntity(UrlForm(("id", id), ("zref", "zref")))
    val response = FrontEndRoutes.ocflRoutes[IO].orNotFound(request)
    def searchContains(text: String) = response.flatMap(_.as[String].map(_.contains(text))).unsafeRunSync() should equal(true)

    searchContains("<caption class=\"govuk-table__caption govuk-table__caption--m\">Search Results</caption>")
    searchContains(s"<a class=\"govuk-link\" href=\"/download/$id\" download=\"zref\">Download</a>")
    searchContains("<th scope=\"row\" class=\"govuk-table__header\">zref</th>")
  }

  "OcflRoutes /search" should "render no results if no results are found" in {
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
