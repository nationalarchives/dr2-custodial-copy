package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.implicits.*
import doobie.util.Get
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reconciler.Database.TableName.{ActualCosInPS, ExpectedCosInPS}
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.{Configuration, Database, OcflCoRow, PreservicaCoRow}
import uk.gov.nationalarchives.utils.TestUtils.*

import java.net.URI
import java.nio.file.{Files, Path}
import java.util.UUID

class DatabaseSpec extends AnyFlatSpec with BeforeAndAfterEach:
  given Get[UUID] = Get[String].map(UUID.fromString)
  case class ReconcilerDatabaseUtils() extends DatabaseUtils("test-reconciler-database") {
    def createOcflCoRow(
        coRef: UUID = UUID.randomUUID,
        ioRef: UUID = UUID.randomUUID,
        sha256Checksum: Option[String] = None,
        sha1Checksum: Option[String] = None,
        md5Checksum: Option[String] = None
    ): IO[OcflCoRow] =
      sql"""INSERT INTO ExpectedCosInPS (ioRef, coRef, representationType, generationType, sha256Checksum, sha1Checksum, md5Checksum)
                 VALUES (${coRef.toString}, ${ioRef.toString}, Preservation, Original, ${sha256Checksum.getOrElse("")}, ${sha1Checksum.getOrElse(
          ""
        )}, ${md5Checksum.getOrElse("")})""".update.run
        .transact(xa)
        .map(_ => OcflCoRow(ioRef, coRef, "Preservation", "Original", sha256Checksum, sha1Checksum, md5Checksum))

    def createPSCoRow(
        coRef: UUID = UUID.randomUUID,
        ioRef: UUID = UUID.randomUUID,
        sha256Checksum: Option[String] = None,
        sha1Checksum: Option[String] = None,
        md5Checksum: Option[String] = None
    ): IO[PreservicaCoRow] =
      sql"""INSERT INTO ActualCosInPS (ioRef, coRef, sha256Checksum, sha1Checksum, md5Checksum)
               VALUES (${coRef.toString}, ${ioRef.toString}, ${sha256Checksum.getOrElse("")}, ${sha1Checksum.getOrElse("")}, ${md5Checksum.getOrElse(
          ""
        )})""".update.run
        .transact(xa)
        .map(_ => PreservicaCoRow(ioRef, coRef, "Original", sha256Checksum, sha1Checksum, md5Checksum))

    def getOcflCoRows(coRef: UUID, tableName: String): IO[List[OcflCoRow]] =
      sql"SELECT * FROM $tableName where coRef = ${coRef.toString}"
        .query[OcflCoRow]
        .to[List]
        .transact(xa)

    def getPreservicaCoRows(coRef: UUID, tableName: String): IO[List[PreservicaCoRow]] =
      sql"SELECT * FROM $tableName where coRef = ${coRef.toString}"
        .query[PreservicaCoRow]
        .to[List]
        .transact(xa)
  }
  val databaseUtils: ReconcilerDatabaseUtils = ReconcilerDatabaseUtils()

  import databaseUtils.*

  override def afterEach(): Unit = Files.delete(Path.of(databaseName))

  given Configuration = new Configuration:
    override def config: Config = Config("", databaseName, 5, Some(URI.create("http://localhost")))

  "writeToExpectedInPsTable" should "should write the values to the ExpectedInPs table" in {
    createExpectedInPsTable()
    val ioRef = UUID.randomUUID
    val coRef = UUID.randomUUID

    val initialResponse = getOcflCoRows(coRef, ExpectedCosInPS.toString).unsafeRunSync()
    val ocflCoRows = List(
      OcflCoRow(ioRef, coRef, "Preservation", "Original", Some("sha256Checksum1"), Some("sha1Checksum1"), Some("md5Checksum1"))
    )
    Database[IO].writeToExpectedInPsTable(ocflCoRows).unsafeRunSync()
    val response = getOcflCoRows(coRef, ExpectedCosInPS.toString).unsafeRunSync()

    initialResponse.isEmpty should equal(true)
    response should equal(ocflCoRows)
  }

  "writeToActuallyInPsTable" should "should write the values to the ActualCosInPS table" in {
    createActuallyInPsTable()
    val ioRef = UUID.randomUUID
    val coRef = UUID.randomUUID

    val initialResponse = getOcflCoRows(coRef, ActualCosInPS.toString).unsafeRunSync()
    val ocflCoRows = List(
      PreservicaCoRow(coRef, ioRef, "Original", Some("sha256Checksum1"), Some("sha1Checksum1"), Some("md5Checksum1"))
    )
    Database[IO].writeToActuallyInPsTable(ocflCoRows).unsafeRunSync()
    val response = getOcflCoRows(coRef, ActualCosInPS.toString).unsafeRunSync()

    initialResponse.isEmpty should equal(true)
    response should equal(ocflCoRows)
  }

  "writeToExpectedInPsTable" should "given a coRef, delete the row and write a new one with the updated values to the ExpectedInPs table" in {
    createExpectedInPsTable()
    val ioRef = UUID.randomUUID
    val coRef = UUID.randomUUID
    val coRef2 = UUID.randomUUID

    val expectedAdditionalCoRow = List(createOcflCoRow(coRef2, ioRef).unsafeRunSync())
    val updatedCoRow = List(createOcflCoRow(coRef, ioRef).unsafeRunSync().copy(representationType = "Access"))

    Database[IO].writeToExpectedInPsTable(updatedCoRow).unsafeRunSync()
    val response = getOcflCoRows(coRef, ExpectedCosInPS.toString).unsafeRunSync()
    val additionalRowResponse = getOcflCoRows(coRef2, ExpectedCosInPS.toString).unsafeRunSync()

    additionalRowResponse.length should equal(1)
    additionalRowResponse.head.representationType should equal("Preservation")
    response.length should equal(1)
    response.head.representationType should equal("Access")
  }

  "writeToActuallyInPsTable" should "given a coRef, delete the row and write a new one with the updated values to the ActualCosInPS table" in {
    createActuallyInPsTable()
    val ioRef = UUID.randomUUID
    val coRef = UUID.randomUUID
    val coRef2 = UUID.randomUUID

    val expectedAdditionalCoRow = List(createPSCoRow(coRef2, ioRef).unsafeRunSync())
    val updatedCoRow = List(createPSCoRow(coRef, ioRef).unsafeRunSync().copy(sha256Checksum = Some("sha256Checksum1")))

    Database[IO].writeToActuallyInPsTable(updatedCoRow).unsafeRunSync()
    val response = getPreservicaCoRows(coRef, ActualCosInPS.toString).unsafeRunSync()
    val additionalRowResponse = getPreservicaCoRows(coRef2, ActualCosInPS.toString).unsafeRunSync()

    additionalRowResponse.length should equal(1)
    additionalRowResponse.head.sha256Checksum should equal(None)
    response.length should equal(1)
    response.head.sha256Checksum should equal(Some("sha256Checksum1"))
  }
  
  "writeToExpectedInPsTable" should "should write nothing to the ExpectedInPs table if no CoRows were passed in" in {
    createExpectedInPsTable()
    val initialResponse = getOcflCoRows(coRef, ExpectedCosInPS.toString).unsafeRunSync()

    Database[IO].writeToExpectedInPsTable(Nil).unsafeRunSync()
    val response = getOcflCoRows(coRef, ExpectedCosInPS.toString).unsafeRunSync()

    initialResponse.isEmpty should equal(true)
    response.isEmpty should equal(true)
  }

  "writeToActuallyInPsTable" should "should write nothing to the ActualCosInPS table if no CoRows were passed in" in {
    createActuallyInPsTable()
    val initialResponse = getOcflCoRows(coRef, ActualCosInPS.toString).unsafeRunSync()

    Database[IO].writeToActuallyInPsTable(Nil).unsafeRunSync()
    val response = getOcflCoRows(coRef, ActualCosInPS.toString).unsafeRunSync()

    initialResponse.isEmpty should equal(true)
    response.isEmpty should equal(true)
  }


  "writeToExpectedInPsTable" should "return an error if there is an error with the ExpectedInPs table or DB" in {
    createExpectedInPsTable()

    val ex = intercept[Exception](Database[IO].writeToExpectedInPsTable(Nil).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: ExpectedCosInPS)")
  }

  "writeToActuallyInPsTable" should "return an error if there is an error with the ActualCosInPS table or DB" in {
    createActuallyInPsTable()

    val ex = intercept[Exception](Database[IO].writeToActuallyInPsTable(Nil).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: ActualCosInPS)")
  }
