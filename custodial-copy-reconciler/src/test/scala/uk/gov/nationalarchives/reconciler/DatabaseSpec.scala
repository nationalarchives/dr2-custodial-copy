package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.Put
import doobie.implicits.*
import doobie.util.Get
import fs2.Chunk
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.{CoRow, Configuration, Database}
import uk.gov.nationalarchives.utils.TestUtils.*

import java.net.URI
import java.nio.file.{Files, Path}
import java.util.UUID

class DatabaseSpec extends AnyFlatSpec with BeforeAndAfterEach:
  given Get[UUID] = Get[String].map(UUID.fromString)
  given Put[UUID] = Put[String].contramap(_.toString)

  case class ReconcilerDatabaseUtils() extends DatabaseUtils("test-database") {
    def createCoRow(
        id: UUID,
        parent: UUID = UUID.randomUUID(),
        sha256Checksum: String
    ): IO[CoRow] =
      sql"""INSERT INTO OcflCOs (id, parent, sha256Checksum)
                 VALUES ($id, $parent, $sha256Checksum)""".update.run
        .transact(xa)
        .map(_ => CoRow(id, Option(parent), sha256Checksum))

    def createPSCoRow(
        id: UUID,
        parent: UUID = UUID.randomUUID(),
        sha256Checksum: String
    ): IO[CoRow] =
      sql"""INSERT INTO PreservicaCOs (id, parent, sha256Checksum)
               VALUES ($id, $parent, $sha256Checksum)""".update.run
        .transact(xa)
        .map(_ => CoRow(id, Option(parent), sha256Checksum))

    def getCoRows(id: UUID): IO[List[CoRow]] =
      sql"SELECT * FROM OcflCOs WHERE id = $id"
        .query[CoRow]
        .to[List]
        .transact(xa)

    def getPreservicaCoRows(id: UUID): IO[List[CoRow]] =
      sql"SELECT * FROM PreservicaCOs WHERE id = $id"
        .query[CoRow]
        .to[List]
        .transact(xa)
  }
  val databaseUtils: ReconcilerDatabaseUtils = ReconcilerDatabaseUtils()

  import databaseUtils.*

  override def afterEach(): Unit = Files.delete(Path.of(databaseName))

  given Configuration = new Configuration:
    override def config: Config = Config("", databaseName, 5, "", "", Some(URI.create("http://localhost")))

  "writeToOcflCOsTable" should "should write the values to the OcflCOs table" in {
    createOcflCOsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()

    val initialResponse = getCoRows(coRef).unsafeRunSync()
    val CoRows = Chunk(
      CoRow(coRef, Option(ioRef), "sha256Checksum1")
    )
    Database[IO].writeToOcflCOsTable(CoRows).unsafeRunSync()
    val response = getCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(CoRows.toList)
  }

  "writeToPreservicaCOsTable" should "should write the values to the PreservicaCOs table" in {
    createPreservicaCOsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()

    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()
    val preservicaCoRows = Chunk(CoRow(coRef, Option(ioRef), "sha256Checksum1"))

    Database[IO].writeToPreservicaCOsTable(preservicaCoRows).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(preservicaCoRows.toList)
  }

  "writeToOcflCOsTable" should "should write nothing to the OcflCOs table if no CoRows were passed in" in {
    createOcflCOsTable()
    val initialResponse = getCoRows(coRef).unsafeRunSync()

    Database[IO].writeToOcflCOsTable(Chunk.empty).unsafeRunSync()
    val response = getCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToPreservicaCOsTable" should "should write nothing to the PreservicaCOs table if no CoRows were passed in" in {
    createPreservicaCOsTable()
    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()

    Database[IO].writeToPreservicaCOsTable(Chunk.empty).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToOcflCOsTable" should "return an error if there is an error with the OcflCOs table or DB" in {
    val ex = intercept[Exception](Database[IO].writeToOcflCOsTable(Chunk.empty).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: OcflCOs)")
  }

  "writeToPreservicaCOsTable" should "return an error if there is an error with the PreservicaCOs table or DB" in {
    val ex = intercept[Exception](Database[IO].writeToPreservicaCOsTable(Chunk.empty).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: PreservicaCOs)")
  }

  "findAllMissingCOs" should s"should return no messages if each CO in PS has a corresponding CO with the same checksum in CC" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    (createCoRow(coRef, ioRef, "checksum1") >> createPSCoRow(coRef, ioRef, "checksum1")).unsafeRunSync()

    val result = Database[IO].findAllMissingCOs().unsafeRunSync()

    result.psCOsCount should equal(1)
    result.psCOsMissingFromCc should be(Nil)
  }

  "findAllMissingCOs" should s"should return no messages if each CO in CC has a corresponding CO with the same checksum in PS" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    (createPSCoRow(coRef, ioRef, "checksum1") >> createCoRow(coRef, ioRef, "checksum1")).unsafeRunSync()

    val result = Database[IO].findAllMissingCOs().unsafeRunSync()

    result.ccCOsCount should equal(1)
    result.ccCOsMissingFromPs should be(Nil)
  }

  "deleteFromTables" should "delete all rows from both tables" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    (createPSCoRow(coRef, ioRef, "checksum1") >> createCoRow(coRef, ioRef, "checksum1")).unsafeRunSync()

    countPreservicaCORows() should equal(1)
    countOcflCORows() should equal(1)

    Database[IO].deleteFromTables().unsafeRunSync()

    countPreservicaCORows() should equal(0)
    countOcflCORows() should equal(0)
  }

  "findAllMissingCOs" should s"should return a message for each CO if the checksums don't match" in {
    val preservicaChecksum = "checksum1"
    val ocflChecksum = "checksum2"
    createPreservicaCOsTable()
    createOcflCOsTable()
    (createPSCoRow(coRef, ioRef, preservicaChecksum) >> createCoRow(coRefTwo, ioRef, ocflChecksum)).unsafeRunSync()

    val result = Database[IO].findAllMissingCOs().unsafeRunSync()

    result.ccCOsMissingFromPs should be(
      List(
        s":alert-noflash-slow: CO $coRefTwo is in CC, but its checksum could not be found in Preservica"
      )
    )

    result.psCOsMissingFromCc should be(
      List(
        s":alert-noflash-slow: CO $coRef is in Preservica, but its checksum could not be found in CC"
      )
    )
  }
