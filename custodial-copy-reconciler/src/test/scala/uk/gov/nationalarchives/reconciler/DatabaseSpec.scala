package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import doobie.implicits.*
import doobie.util.{Get, Put}
import fs2.Chunk
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor3
import org.scalatest.prop.Tables.Table
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.{Configuration, Database, OcflCoRow, PreservicaCoRow}
import uk.gov.nationalarchives.utils.TestUtils.*

import java.net.URI
import java.nio.file.{Files, Path}
import java.util.UUID

class DatabaseSpec extends AnyFlatSpec with BeforeAndAfterEach:
  given Get[UUID] = Get[String].map(UUID.fromString)
  given Put[UUID] = Put[String].contramap(_.toString)

  case class ReconcilerDatabaseUtils() extends DatabaseUtils("test-database") {
    def createOcflCoRow(
        coRef: UUID,
        ioRef: UUID = UUID.randomUUID(),
        sha256Checksum: Option[String] = None
    ): IO[OcflCoRow] =
      sql"""INSERT INTO OcflRows (coRef, ioRef, sha256Checksum)
                 VALUES ($coRef, $ioRef, $sha256Checksum)""".update.run
        .transact(xa)
        .map(_ => OcflCoRow(coRef, ioRef, sha256Checksum))

    def createPsCoRow(
        coRef: UUID,
        ioRef: UUID = UUID.randomUUID(),
        sha256Checksum: Option[String] = None
    ): IO[PreservicaCoRow] =
      sql"""INSERT INTO PsRows (coRef, ioRef, sha256Checksum)
               VALUES ($coRef, $ioRef, $sha256Checksum)""".update.run
        .transact(xa)
        .map(_ => PreservicaCoRow(coRef, ioRef, sha256Checksum))

    def getOcflCoRows(coRef: UUID): IO[List[OcflCoRow]] =
      sql"SELECT * FROM OcflRows WHERE coRef = $coRef"
        .query[OcflCoRow]
        .to[List]
        .transact(xa)

    def getPreservicaCoRows(coRef: UUID): IO[List[PreservicaCoRow]] =
      sql"SELECT * FROM PsRows WHERE coRef = $coRef"
        .query[PreservicaCoRow]
        .to[List]
        .transact(xa)
  }

  val databaseUtils: ReconcilerDatabaseUtils = ReconcilerDatabaseUtils()

  import databaseUtils.*

  override def afterEach(): Unit = Files.delete(Path.of(databaseName))

  given Configuration = new Configuration:
    override def config: Config = Config("", databaseName, 5, "", "", Some(URI.create("http://localhost")))

  "writeToExpectedInPsTable" should "should write the values to the ExpectedInPs table" in {
    createOcflTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()

    val initialResponse = getOcflCoRows(coRef).unsafeRunSync()
    val ocflCoRows = Chunk(
      OcflCoRow(coRef, ioRef, Some("sha256Checksum1"))
    )
    Database[IO].writeToOcflTable(ocflCoRows).unsafeRunSync()
    val response = getOcflCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(ocflCoRows.toList)
  }

  "writeToActuallyInPsTable" should "should write the values to the PsRows table" in {
    createPsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()

    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()
    val preservicaCoRows = Chunk(PreservicaCoRow(coRef, ioRef, Some("sha256Checksum1")))

    Database[IO].writeToPsTable(preservicaCoRows).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(preservicaCoRows.toList)
  }

  "writeToExpectedInPsTable" should "should write nothing to the OcflRows table if no CoRows were passed in" in {
    createOcflTable()
    val initialResponse = getOcflCoRows(coRef).unsafeRunSync()

    Database[IO].writeToOcflTable(Chunk.empty).unsafeRunSync()
    val response = getOcflCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToActuallyInPsTable" should "should write nothing to the PsRows table if no CoRows were passed in" in {
    createPsTable()
    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()

    Database[IO].writeToPsTable(Chunk.empty).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToExpectedInPsTable" should "return an error if there is an error with the ExpectedInPs table or DB" in {
    val ex = intercept[Exception](Database[IO].writeToOcflTable(Chunk.empty).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: OcflRows)")
  }

  "writeToActuallyInPsTable" should "return an error if there is an error with the PsRows table or DB" in {
    val ex = intercept[Exception](Database[IO].writeToPsTable(Chunk.empty).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: PsRows)")
  }

  val checksumMatchPossibilities: TableFor3[Option[String], Option[String], Boolean] = Table(
    ("ocflChecksum", "preservicaChecksum", "matches"),
    ("checksum1".some, "checksum1".some, true),
    ("checksum1".some, "checksum2".some, false),
    ("checksum2".some, "checksum1".some, false),
    (None, "checksum1".some, false),
    ("checksum1".some, None, false),
    (None, None, false)
  )

  forAll(checksumMatchPossibilities) { (ocflChecksum, psChecksum, matches) =>
    "findAllMissingFiles" should s"should ${if matches then "" else "not"} return a message for ocfl checksum $ocflChecksum and psChecksum $psChecksum" in {
      createOcflTable()
      createPsTable()
      (createOcflCoRow(coRef, ioRef, ocflChecksum) >> createPsCoRow(coRef, ioRef, psChecksum)).unsafeRunSync()

      val coMessage = Database[IO].findAllMissingFiles().unsafeRunSync()

      coMessage.isEmpty should be(matches)
    }
  }
