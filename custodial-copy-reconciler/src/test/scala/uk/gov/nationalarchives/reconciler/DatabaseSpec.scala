package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.implicits.*
import doobie.util.Get
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.prop.{TableFor4, TableFor5}
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
        sha256Checksum: Option[String] = None,
        sha1Checksum: Option[String] = None,
        md5Checksum: Option[String] = None
    ): IO[OcflCoRow] =
      sql"""INSERT INTO ExpectedCosInPS (coRef, ioRef, representationType, generationType, sha256Checksum, sha1Checksum, md5Checksum)
                 VALUES (${coRef.toString}, ${ioRef.toString}, "Preservation_1", "Original", $sha256Checksum, $sha1Checksum, $md5Checksum)""".update.run
        .transact(xa)
        .map(_ => OcflCoRow(coRef, ioRef, "Preservation_1", "Original", sha256Checksum, sha1Checksum, md5Checksum))

    def createPSCoRow(
        coRef: UUID,
        ioRef: UUID = UUID.randomUUID(),
        sha256Checksum: Option[String] = None,
        sha1Checksum: Option[String] = None,
        md5Checksum: Option[String] = None
    ): IO[PreservicaCoRow] =
      sql"""INSERT INTO ActualCosInPS (coRef, ioRef, generationType, sha256Checksum, sha1Checksum, md5Checksum)
               VALUES (${coRef.toString}, ${ioRef.toString}, "Original", $sha256Checksum, $sha1Checksum, $md5Checksum)""".update.run
        .transact(xa)
        .map(_ => PreservicaCoRow(coRef, ioRef, "Original", sha256Checksum, sha1Checksum, md5Checksum))

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
      OcflCoRow(coRef, ioRef, "Preservation_1", "Original", Some("sha256Checksum1"), Some("sha1Checksum1"), Some("md5Checksum1"))
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
    val preservicaCoRows = List(PreservicaCoRow(coRef, ioRef, "Original", Some("sha256Checksum1"), None, None))

    Database[IO].writeToActuallyInPsTable(preservicaCoRows).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(preservicaCoRows)
  }

  "writeToExpectedInPsTable" should "given a coRef, delete the row and write a new one with the updated values to the ExpectedInPs table" in {
    createExpectedInPsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()
    val coRef2 = UUID.randomUUID()

    val expectedAdditionalCoRow = List(createOcflCoRow(coRef2, ioRef).unsafeRunSync())
    val updatedCoRow = List(createOcflCoRow(coRef, ioRef).unsafeRunSync().copy(representationType = "Access_1"))

    Database[IO].writeToExpectedInPsTable(updatedCoRow).unsafeRunSync()
    val response = getOcflCoRows(coRef).unsafeRunSync()
    val additionalRowResponse = getOcflCoRows(coRef2).unsafeRunSync()

    additionalRowResponse.length should equal(1)
    additionalRowResponse.head.representationType should equal("Preservation_1")
    additionalRowResponse.head.coRef should equal(coRef2)

    response.length should equal(1)
    response.head.representationType should equal("Access_1")
    response.head.coRef should equal(coRef)
  }

  "writeToActuallyInPsTable" should "given a coRef, delete the row and write a new one with the updated values to the ActualCosInPS table" in {
    createActuallyInPsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()
    val coRef2 = UUID.randomUUID()

    val updatedCoRow = List(createPSCoRow(coRef, ioRef).unsafeRunSync().copy(sha256Checksum = Some("sha256Checksum1")))
    val expectedAdditionalCoRow = List(createPSCoRow(coRef2, ioRef).unsafeRunSync())

    Database[IO].writeToActuallyInPsTable(updatedCoRow).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()
    val additionalRowResponse = getPreservicaCoRows(coRef2).unsafeRunSync()

    additionalRowResponse.length should equal(1)
    additionalRowResponse.head.sha256Checksum should equal(None)
    additionalRowResponse.head.coRef should equal(coRef2)

    response.length should equal(1)
    response.head.coRef should equal(coRef)
    response.head.sha256Checksum should equal(Some("sha256Checksum1"))
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

  val checksumMatchPossibilities
      : TableFor5[String, Option[String], Option[String], Option[String], TableFor4[String, Option[String], Option[String], Option[String]]] = Table(
    ("checksumMatch", "sha256Checksum", "sha1Checksum", "md5Checksum", "checksumMismatches"),
    (
      "sha256",
      Some("sha256Checksum1"),
      None,
      None,
      Table(
        ("checksumMismatch", "sha256Checksum", "sha1Checksum", "md5Checksum"),
        ("sha1", None, Some("sha1Checksum1"), None),
        ("md5", None, None, Some("md5Checksum1")),
        ("sha256, sha1 & md5", None, None, None)
      )
    ),
    (
      "sha1",
      None,
      Some("sha1Checksum1"),
      None,
      Table(
        ("checksumMismatch", "sha256Checksum", "sha1Checksum", "md5Checksum"),
        ("sha256", Some("sha256Checksum1"), None, None),
        ("md5", None, None, Some("md5Checksum1")),
        ("sha256, sha1 & md5", None, None, None)
      )
    ),
    (
      "md5",
      None,
      None,
      Some("md5Checksum1"),
      Table(
        ("checksumMismatch", "sha256Checksum", "sha1Checksum", "md5Checksum"),
        ("sha256", Some("sha256Checksum1"), None, None),
        ("sha1", None, Some("sha1Checksum1"), None),
        ("sha256, sha1 & md5", None, None, None)
      )
    ),
    (
      "sha256 & sha1",
      Some("sha256Checksum1"),
      Some("sha1Checksum1"),
      None,
      Table(
        ("checksumMismatch", "sha256Checksum", "sha1Checksum", "md5Checksum"),
        ("md5", None, None, Some("md5Checksum1")),
        ("sha256, sha1 & md5", None, None, None)
      )
    ),
    (
      "sha1 & md5",
      None,
      Some("sha1Checksum1"),
      Some("md5Checksum1"),
      Table(
        ("checksumMismatch", "sha256Checksum", "sha1Checksum", "md5Checksum"),
        ("sha256", Some("sha256Checksum1"), None, None),
        ("sha256, sha1 & md5", None, None, None)
      )
    ),
    (
      "sha256 & md5",
      Some("sha256Checksum1"),
      None,
      Some("md5Checksum1"),
      Table(
        ("checksumMismatch", "sha256Checksum", "sha1Checksum", "md5Checksum"),
        ("sha256", None, Some("sha1Checksum1"), None),
        ("sha256, sha1 & md5", None, None, None)
      )
    ),
    (
      "sha256, sha1 & md5",
      Some("sha256Checksum1"),
      Some("sha1Checksum1"),
      Some("md5Checksum1"),
      Table(
        ("checksumMismatch", "sha256Checksum", "sha1Checksum", "md5Checksum"),
        ("sha256, sha1 & md5", None, None, None)
      )
    )
  )

  forAll(checksumMatchPossibilities) { (checksumMatch, sha256Checksum, sha1Checksum, md5Checksum, _) =>
    "checkIfPsCoInCc" should s"should return a message for each CO if it has the same ${checksumMatch} checksum in CC" in {
      createExpectedInPsTable()
      createOcflCoRow(coRef, ioRef, sha256Checksum, sha1Checksum, md5Checksum).unsafeRunSync()

      val preservicaCoRow = PreservicaCoRow(coRef, ioRef, "Original", sha256Checksum, sha1Checksum, md5Checksum)
      val coMessage = Database[IO].checkIfPsCoInCc(preservicaCoRow).unsafeRunSync()

      coMessage should be(Nil)
    }

    "checkIfCcCoInPs" should s"should return a message for each CO if it has the same ${checksumMatch} checksum in PS" in {
      createActuallyInPsTable()
      createPSCoRow(coRef, ioRef, sha256Checksum, sha1Checksum, md5Checksum).unsafeRunSync()

      val ocflCoRow = OcflCoRow(coRef, ioRef, "Preservica_1", "Original", sha256Checksum, sha1Checksum, md5Checksum)
      val coMessage = Database[IO].checkIfCcCoInPs(ocflCoRow).unsafeRunSync()

      coMessage should be(Nil)
    }
  }

  forAll(checksumMatchPossibilities) { (checksumMatch, sha256Checksum, sha1Checksum, md5Checksum, checksumMismatchPossibilities) =>
    forAll(checksumMismatchPossibilities) { (checksumMismatch, sha256ChecksumMismatch, sha1ChecksumMismatch, md5ChecksumMismatch) =>

      "checkIfPsCoInCc" should s"should return a message for each CO if it has a ${checksumMatch} checksum in PS but a ${checksumMismatch} checksum in CC" in {
        createExpectedInPsTable()
        createOcflCoRow(coRef, ioRef, sha256Checksum, sha1Checksum, md5Checksum).unsafeRunSync()

        val preservicaCoRow = PreservicaCoRow(coRef, ioRef, "Original", sha256ChecksumMismatch, sha1ChecksumMismatch, md5ChecksumMismatch)
        val coMessage = Database[IO].checkIfPsCoInCc(preservicaCoRow).unsafeRunSync()

        coMessage should be(List(s"CO $coRef (parent: $ioRef) is in Preservica, but its checksum could not be found in CC"))
      }

      "checkIfCcCoInPs" should s"should return a message for each CO if it has a ${checksumMatch} checksum in CC but a ${checksumMismatch} checksum in PS" in {
        createActuallyInPsTable()
        createPSCoRow(coRef, ioRef, sha256Checksum, sha1Checksum, md5Checksum).unsafeRunSync()

        val ocflCoRow = OcflCoRow(coRef, ioRef, "Preservica_1", "Original", sha256ChecksumMismatch, sha1ChecksumMismatch, md5ChecksumMismatch)
        val coMessage = Database[IO].checkIfCcCoInPs(ocflCoRow).unsafeRunSync()

        coMessage should be(List(s"CO $coRef (parent: $ioRef) is in CC, but its checksum could not be found in Preservica"))
      }
    }
  }
