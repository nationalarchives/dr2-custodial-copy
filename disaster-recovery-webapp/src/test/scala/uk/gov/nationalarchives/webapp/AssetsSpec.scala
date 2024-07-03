package uk.gov.nationalarchives.webapp

import cats.effect.IO
import cats.implicits.*
import uk.gov.nationalarchives.webapp.FrontEndRoutes.SearchResponse
import uk.gov.nationalarchives.utils.TestUtils.*
import uk.gov.nationalarchives.utils.Utils.OcflFile
import doobie.Transactor
import doobie.implicits.*
import org.scalatest.flatspec.AnyFlatSpec
import cats.effect.unsafe.implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers.*

import java.nio.file.{Files, Paths}
import java.util.UUID

class AssetsSpec extends AnyFlatSpec with BeforeAndAfterAll:

  override def beforeAll(): Unit = createTable()

  private def ocflFile(id: String, fileId: String, zref: String = "zref") = OcflFile(1, id, "name", fileId, zref, "path", "fileName")

  override def afterAll(): Unit = Files.delete(Paths.get("test-database"))

  "filePath" should "return a file path if the id is in the database" in {
    val id = UUID.randomUUID
    val path = for {
      _ <- createFile(id.toString)
      path <- Assets[IO].filePath(id)
    } yield path
    path.unsafeRunSync() should equal("path")
  }

  "filePath" should "return an error if the id is not in the database" in {
    val id = UUID.randomUUID
    val ex = intercept[Exception] {
      Assets[IO].filePath(id).unsafeRunSync()
    }
    ex.getMessage should equal(s"Id $id not found in the database")
  }

  "findFiles" should "return the correct file if only the id is provided" in {
    for {
      file <- createFile()
      files <- Assets[IO].findFiles(SearchResponse(file.id.some, None))
    } yield {
      files.size should equal(1)
      files should equal(List(file))
    }
  }.unsafeRunSync()

  "findFiles" should "return the correct file if the id and zref are provided" in {
    for {
      file <- createFile(zref = "testZref")
      files <- Assets[IO].findFiles(SearchResponse(file.id.some, "testZref".some))
    } yield files should equal(List(file))
  }.unsafeRunSync()

  "findFiles" should "return the correct file if only the zref is provided" in {
    for {
      file <- createFile(zref = "anotherTestZref")
      files <- Assets[IO].findFiles(SearchResponse(None, "anotherTestZref".some))
    } yield files should equal(List(file))
  }.unsafeRunSync()

  "findFiles" should "return an empty list if there are no matching entries in the database" in {
    for {
      files <- Assets[IO].findFiles(SearchResponse("id".some, "zref".some))
    } yield files.isEmpty should equal(true)
  }.unsafeRunSync()
