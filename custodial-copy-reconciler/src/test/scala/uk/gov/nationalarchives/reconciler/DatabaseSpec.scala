package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.Deferred
import cats.effect.std.Mutex
import cats.effect.unsafe.implicits.global
import org.typelevel.doobie.Put
import org.typelevel.doobie.implicits.*
import org.typelevel.doobie.util.Get
import fs2.Chunk
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reconciler.Database.CoRow
import uk.gov.nationalarchives.reconciler.Database.given
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.reconciler.{Configuration, Database}
import uk.gov.nationalarchives.utils.TestUtils.*

import java.net.URI
import java.nio.file.{Files, Path}
import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.duration.*

class DatabaseSpec extends AnyFlatSpec with BeforeAndAfterEach:
  given Get[UUID] = Get[String].map(UUID.fromString)
  given Put[UUID] = Put[String].contramap(_.toString)

  case class ReconcilerDatabaseUtils() extends DatabaseUtils("test-database") {
    def createCoRow(
        id: UUID,
        parent: UUID = UUID.randomUUID(),
        sha256Checksum: String,
        createdDate: OffsetDateTime
    ): IO[CoRow] =
      sql"""INSERT INTO OcflCOs (id, parent, sha256Checksum, createdDate)
                 VALUES ($id, $parent, $sha256Checksum, $createdDate)""".update.run
        .transact(xa)
        .map(_ => CoRow(id, Option(parent), sha256Checksum, createdDate))

    def createPSCoRow(
        id: UUID,
        parent: UUID = UUID.randomUUID(),
        sha256Checksum: String,
        createdDate: OffsetDateTime
    ): IO[CoRow] =
      sql"""INSERT INTO PreservicaCOs (id, parent, sha256Checksum, createdDate)
               VALUES ($id, $parent, $sha256Checksum, $createdDate)""".update.run
        .transact(xa)
        .map(_ => CoRow(id, Option(parent), sha256Checksum, createdDate))

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
    override def config: Config = Config("", databaseName, 5, "", "", 0, Some(URI.create("http://localhost")))

  val mutex: Mutex[IO] = Mutex[IO].unsafeRunSync()

  "writeToOcflCOsTable" should "should write the values to the OcflCOs table" in {
    createOcflCOsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()
    val createdDate = OffsetDateTime.now
    val initialResponse = getCoRows(coRef).unsafeRunSync()
    val CoRows = Chunk(
      CoRow(coRef, Option(ioRef), "sha256Checksum1", createdDate)
    )
    Database[IO](mutex).writeToOcflCOsTable(CoRows).unsafeRunSync()
    val response = getCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(CoRows.toList)
  }

  "writeToPreservicaCOsTable" should "should write the values to the PreservicaCOs table" in {
    createPreservicaCOsTable()
    val ioRef = UUID.randomUUID()
    val coRef = UUID.randomUUID()
    val createdDate = OffsetDateTime.now

    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()
    val preservicaCoRows = Chunk(CoRow(coRef, Option(ioRef), "sha256Checksum1", createdDate))

    Database[IO](mutex).writeToPreservicaCOsTable(preservicaCoRows).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(preservicaCoRows.toList)
  }

  "writeToOcflCOsTable" should "should write nothing to the OcflCOs table if no CoRows were passed in" in {
    createOcflCOsTable()
    val initialResponse = getCoRows(coRef).unsafeRunSync()

    Database[IO](mutex).writeToOcflCOsTable(Chunk.empty).unsafeRunSync()
    val response = getCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToPreservicaCOsTable" should "should write nothing to the PreservicaCOs table if no CoRows were passed in" in {
    createPreservicaCOsTable()
    val initialResponse = getPreservicaCoRows(coRef).unsafeRunSync()

    Database[IO](mutex).writeToPreservicaCOsTable(Chunk.empty).unsafeRunSync()
    val response = getPreservicaCoRows(coRef).unsafeRunSync()

    initialResponse should equal(Nil)
    response should equal(Nil)
  }

  "writeToOcflCOsTable" should "return an error if there is an error with the OcflCOs table or DB" in {
    val ex = intercept[Exception](Database[IO](mutex).writeToOcflCOsTable(Chunk.empty).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: OcflCOs)")
  }

  "writeToPreservicaCOsTable" should "return an error if there is an error with the PreservicaCOs table or DB" in {
    val ex = intercept[Exception](Database[IO](mutex).writeToPreservicaCOsTable(Chunk.empty).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: PreservicaCOs)")
  }

  "findAllMissingCOs" should s"should return no messages if each CO in PS has a corresponding CO with the same checksum in CC" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    val createdDate = OffsetDateTime.now
    (createCoRow(coRef, ioRef, "checksum1", createdDate) >> createPSCoRow(coRef, ioRef, "checksum1", createdDate)).unsafeRunSync()

    val result = Database[IO](mutex).findAllMissingCOs(createdDate).unsafeRunSync()

    result.psCOsCount should equal(1)
    result.psCOsMissingFromCc should be(Nil)
  }

  "findAllMissingCOs" should s"should return no messages if each CO in CC has a corresponding CO with the same checksum in PS" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    val createdDate = OffsetDateTime.now
    (createPSCoRow(coRef, ioRef, "checksum1", createdDate) >> createCoRow(coRef, ioRef, "checksum1", createdDate)).unsafeRunSync()

    val result = Database[IO](mutex).findAllMissingCOs(createdDate).unsafeRunSync()

    result.ccCOsCount should equal(1)
    result.ccCOsMissingFromPs should be(Nil)
  }

  "deleteFromTables" should "delete all rows from both tables" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    val createdDate = OffsetDateTime.now
    (createPSCoRow(coRef, ioRef, "checksum1", createdDate) >> createCoRow(coRef, ioRef, "checksum1", createdDate)).unsafeRunSync()

    countPreservicaCORows() should equal(1)
    countOcflCORows() should equal(1)

    Database[IO](mutex).deleteFromTables().unsafeRunSync()

    countPreservicaCORows() should equal(0)
    countOcflCORows() should equal(0)
  }

  "findAllMissingCOs" should s"should return a message for each CO if the checksums don't match" in {
    val preservicaChecksum = "checksum1"
    val ocflChecksum = "checksum2"
    val createdDate = OffsetDateTime.now
    createPreservicaCOsTable()
    createOcflCOsTable()
    (
      createPSCoRow(coRef, ioRef, preservicaChecksum, createdDate.plusDays(1)) >>
        createCoRow(coRefTwo, ioRef, ocflChecksum, createdDate)
    ).unsafeRunSync()

    val result = Database[IO](mutex).findAllMissingCOs(createdDate.plusDays(2)).unsafeRunSync()

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

  "findAllMissingCOs" should "exclude recent Preservica rows but include old rows" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    val endDate = OffsetDateTime.now
    val recentCoRef = UUID.randomUUID
    val oldCoRef = UUID.randomUUID

    (
      createPSCoRow(recentCoRef, sha256Checksum = "recent", createdDate = endDate.plusDays(1)) >>
        createPSCoRow(oldCoRef, sha256Checksum = "old", createdDate = endDate.minusDays(1))
    ).unsafeRunSync()

    val result = Database[IO](mutex).findAllMissingCOs(endDate).unsafeRunSync()

    result.psCOsMissingFromCc should equal(
      List(s":alert-noflash-slow: CO $oldCoRef is in Preservica, but its checksum could not be found in CC")
    )
  }

  "findAllMissingCOs" should "exclude OCFL rows created after the latest Preservica row" in {
    createPreservicaCOsTable()
    createOcflCOsTable()
    val latestPreservicaDate = OffsetDateTime.now
    val oldOcflCoRef = UUID.randomUUID
    val recentOcflCoRef = UUID.randomUUID
    val matchingCoRef = UUID.randomUUID

    (
      createPSCoRow(matchingCoRef, sha256Checksum = "matching", createdDate = latestPreservicaDate) >>
        createCoRow(matchingCoRef, sha256Checksum = "matching", createdDate = latestPreservicaDate) >>
        createCoRow(oldOcflCoRef, sha256Checksum = "old", createdDate = latestPreservicaDate.minusDays(1)) >>
        createCoRow(recentOcflCoRef, sha256Checksum = "recent", createdDate = latestPreservicaDate.plusDays(1))
    ).unsafeRunSync()

    val result = Database[IO](mutex).findAllMissingCOs(latestPreservicaDate.plusDays(2)).unsafeRunSync()

    result.ccCOsMissingFromPs should equal(
      List(s":alert-noflash-slow: CO $oldOcflCoRef is in CC, but its checksum could not be found in Preservica")
    )
  }

  "Database operations" should "wait for the shared mutex before writing" in {
    createPreservicaCOsTable()
    val database = Database[IO](mutex)
    val coRef = UUID.randomUUID
    val row = CoRow(coRef, None, "checksum", OffsetDateTime.now)
    val completed = Deferred[IO, Unit].unsafeRunSync()

    val write = database
      .writeToPreservicaCOsTable(Chunk.singleton(row))
      .guarantee(completed.complete(()).void)

    val (writeFiber, completedWhileLocked) = mutex.lock
      .surround(
        for
          fiber <- write.start
          _ <- IO.sleep(100.millis)
          completion <- completed.tryGet
        yield (fiber, completion)
      )
      .unsafeRunSync()

    completedWhileLocked should equal(None)
    writeFiber.join.unsafeRunSync()
    completed.tryGet.unsafeRunSync() should equal(Some(()))
    getPreservicaCoRows(coRef).unsafeRunSync() should equal(List(row))
  }
