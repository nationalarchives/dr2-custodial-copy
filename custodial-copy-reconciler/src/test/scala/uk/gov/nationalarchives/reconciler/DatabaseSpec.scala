package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.implicits.*
import doobie.util.Get
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
  case class ReconcilerDatabaseUtils() extends DatabaseUtils("test-database") {
    def createOcflCoRow(
        coRef: UUID,
        ioRef: UUID = UUID.randomUUID(),
        sha256Checksum: Option[String] = None
    ): IO[OcflCoRow] =
      sql"""INSERT INTO ExpectedCosInPS (coRef, ioRef, sha256Checksum)
                 VALUES (${coRef.toString}, ${ioRef.toString}, $sha256Checksum)""".update.run
        .transact(xa)
        .map(_ => OcflCoRow(coRef, ioRef, sha256Checksum))

    def createPSCoRow(
        coRef: UUID,
        ioRef: UUID = UUID.randomUUID(),
        sha256Checksum: Option[String] = None
    ): IO[PreservicaCoRow] =
      sql"""INSERT INTO ActualCosInPS (coRef, ioRef, sha256Checksum)
               VALUES (${coRef.toString}, ${ioRef.toString}, $sha256Checksum)""".update.run
        .transact(xa)
        .map(_ => PreservicaCoRow(coRef, ioRef, sha256Checksum))

    def getOcflCoRows(coRef: UUID): IO[List[OcflCoRow]] =
      sql"SELECT * FROM ExpectedCosInPS WHERE coRef = ${coRef.toString}"
        .query[OcflCoRow]
        .to[List]
        .transact(xa)

    def getPreservicaCoRows(coRef: UUID): IO[List[PreservicaCoRow]] =
      sql"SELECT * FROM ActualCosInPS WHERE coRef = ${coRef.toString}"
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
    createExpectedInPsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()

    val initialResponse = getOcflCoRows(coRef).unsafeRunSync()
    val ocflCoRows = List(
      OcflCoRow(coRef, ioRef, Some("sha256Checksum1"))
    )
    Database[IO].writeToExpectedInPsTable(ocflCoRows).unsafeRunSync()
    val response = getOcflCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(ocflCoRows)
  }

  "writeToActuallyInPsTable" should "should write the values to the ActualCosInPS table" in {
    createActuallyInPsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()

    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()
    val preservicaCoRows = List(PreservicaCoRow(coRef, ioRef, Some("sha256Checksum1")))

    Database[IO].writeToActuallyInPsTable(preservicaCoRows).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(preservicaCoRows)
  }

  "writeToExpectedInPsTable" should "should write nothing to the ExpectedInPs table if no CoRows were passed in" in {
    createExpectedInPsTable()
    val initialResponse = getOcflCoRows(coRef).unsafeRunSync()

    Database[IO].writeToExpectedInPsTable(Nil).unsafeRunSync()
    val response = getOcflCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToActuallyInPsTable" should "should write nothing to the ActualCosInPS table if no CoRows were passed in" in {
    createActuallyInPsTable()
    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()

    Database[IO].writeToActuallyInPsTable(Nil).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToExpectedInPsTable" should "return an error if there is an error with the ExpectedInPs table or DB" in {
    val ex = intercept[Exception](Database[IO].writeToExpectedInPsTable(Nil).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: ExpectedCosInPS)")
  }

  "writeToActuallyInPsTable" should "return an error if there is an error with the ActualCosInPS table or DB" in {
    val ex = intercept[Exception](Database[IO].writeToActuallyInPsTable(Nil).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: ActualCosInPS)")
  }

  val checksumMismatchPossibilities: TableFor3[String, Option[String], Option[String]] = Table(
    ("Mismatch", "ocflChecksum", "preservicaChecksum"),
    ("checksums exist in both tables, but they're different", Some("checksum1"), Some("checksum2")),
    ("checksums doesn't exist in OCFL but does in Preservica", None, Some("checksum1")),
    ("checksums does exist in OCFL but doesn't in Preservica", Some("checksum1"), None),
    ("checksums doesn't exist in both tables", None, None)
  )

  "checkIfPsCoInCc" should s"should return a message for each CO if it has the same checksum in CC" in {
    createExpectedInPsTable()
    createOcflCoRow(coRef, ioRef, Some("checksum1")).unsafeRunSync()

    val preservicaCoRow = PreservicaCoRow(coRef, ioRef, Some("checksum1"))
    val coMessage = Database[IO].checkIfPsCoInCc(preservicaCoRow).unsafeRunSync()

    coMessage should be(Nil)
  }

  "checkIfCcCoInPs" should s"should return a message for each CO if it has the same checksum in PS" in {
    createActuallyInPsTable()
    createPSCoRow(coRef, ioRef, Some("checksum1")).unsafeRunSync()

    val ocflCoRow = OcflCoRow(coRef, ioRef, Some("checksum1"))
    val coMessage = Database[IO].checkIfCcCoInPs(ocflCoRow).unsafeRunSync()

    coMessage should be(Nil)
  }


  forAll(checksumMismatchPossibilities) { (mismatch, ocflChecksum, preservicaChecksum) =>
    "checkIfPsCoInCc" should s"should return a message for each CO if $mismatch" in {
      createExpectedInPsTable()
      createOcflCoRow(coRef, ioRef, ocflChecksum).unsafeRunSync()

      val preservicaCoRow = PreservicaCoRow(coRef, ioRef, preservicaChecksum)
      val coMessage = Database[IO].checkIfPsCoInCc(preservicaCoRow).unsafeRunSync()

      coMessage should be(List(s"CO $coRef (parent: $ioRef) is in Preservica, but its checksum could not be found in CC"))
    }

    "checkIfCcCoInPs" should s"should return a message for each CO if $mismatch" in {
      createActuallyInPsTable()
      createPSCoRow(coRef, ioRef, preservicaChecksum).unsafeRunSync()

      val ocflCoRow = OcflCoRow(coRef, ioRef, ocflChecksum)
      val coMessage = Database[IO].checkIfCcCoInPs(ocflCoRow).unsafeRunSync()

      coMessage should be(List(s"CO $coRef (parent: $ioRef) is in CC, but its checksum could not be found in Preservica"))
    }
  }
