package uk.gov.nationalarchives.webapp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import uk.gov.nationalarchives.utils.TestUtils.*
import uk.gov.nationalarchives.utils.Utils.OcflFile
import uk.gov.nationalarchives.webapp.FrontEndRoutes.SearchResponse

import java.nio.file.{Files, Paths}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID

class AssetsSpec extends AnyFlatSpec with BeforeAndAfterAll with ScalaCheckDrivenPropertyChecks:

  val databaseUtils = new DatabaseUtils("test-database")
  import databaseUtils.*

  private val ingestDateTime: Instant = Instant.now()

  override def beforeAll(): Unit = createTable()

  private def ocflFile(id: UUID, fileId: UUID, zref: String = "zref") =
    OcflFile(1, id, "name".some, fileId, zref.some, "path".some, "fileName".some, ingestDateTime.some, "sourceId".some, "citation".some, "TDR-2025-RNDM".some)

  override def afterAll(): Unit = Files.delete(Paths.get("test-database"))

  "filePath" should "return a file path if the id is in the database" in {
    val id = UUID.randomUUID
    val path = for {
      _ <- createFile(id)
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

  def generatedSearchResponses: Gen[SearchResponse] = for {
    instant <- Gen.option(Gen.choose(Instant.EPOCH.getEpochSecond, Instant.now().getEpochSecond))
    id <- Gen.option(Gen.uuid)
    zref <- Gen.option(Gen.alphaNumStr)
    sourceId <- Gen.option(Gen.alphaNumStr)
    citation <- Gen.option(Gen.alphaNumStr)
    consignmentRef <- Gen.option(consignmentRefPattern)
  } yield SearchResponse(id, zref, sourceId, citation, instant.map(Instant.ofEpochSecond), consignmentRef)

  def consignmentRefPattern: Gen[String] = for {
    system <- Gen.listOfN(3, Gen.alphaUpperChar).map(_.mkString)
    year <- Gen.choose(2020, 2026)
    randomLetters <- Gen.listOfN(4, Gen.alphaUpperChar).map(_.mkString)
  } yield s"$system-$year-$randomLetters"

  forAll(generatedSearchResponses) { searchResponse =>
    "findFiles" should s"return the correct file for search $searchResponse" in {
      val id = searchResponse.id.getOrElse(UUID.randomUUID)
      (for {
        file <- createFile(
          id,
          searchResponse.zref,
          searchResponse.sourceId,
          searchResponse.citation,
          searchResponse.ingestDateTime,
          searchResponse.consignmentRef
        )
        files <- Assets[IO].findFiles(searchResponse)
      } yield {
        files.size should equal(1)
        files.head should equal(file)
      }).unsafeRunSync()
    }
  }

  "findFiles" should "return an entry if the database row has an ingest time on the same day as the search parameters" in {
    val id = UUID.randomUUID
    val dbInstant = LocalDateTime.of(2024, 7, 9, 17, 26, 30).toInstant(ZoneOffset.UTC)
    val searchInstant = LocalDateTime.of(2024, 7, 9, 0, 0, 0).toInstant(ZoneOffset.UTC)
    (for {
      file <- createFile(id, "zref".some, "sourceId".some, "citation".some, dbInstant.some, "TDR-2025-RNDM".some)
      files <- Assets[IO].findFiles(SearchResponse(None, None, None, None, searchInstant.some, None))
    } yield {
      files.size should equal(1)
    }).unsafeRunSync()
  }

  "findFiles" should "return an empty list if there are no matching entries in the database" in {
    for {
      files <- Assets[IO].findFiles(SearchResponse(UUID.randomUUID.some, "zref".some, None, None, None, None))
    } yield files.isEmpty should equal(true)
  }.unsafeRunSync()
