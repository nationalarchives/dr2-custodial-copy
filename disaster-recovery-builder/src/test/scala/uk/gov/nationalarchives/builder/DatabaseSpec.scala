package uk.gov.nationalarchives.builder

import cats.effect.IO
import cats.syntax.all.*
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.builder.Main.Config
import uk.gov.nationalarchives.utils.TestUtils.{createFile, createTable, ocflFile, readFiles}
import uk.gov.nationalarchives.utils.Utils.OcflFile
import cats.effect.unsafe.implicits.global
import org.scalatest.matchers.should.Matchers.*

import java.nio.file.{Files, Path}
import java.util.UUID

class DatabaseSpec extends AnyFlatSpec with BeforeAndAfterEach:

  override def afterEach(): Unit = Files.delete(Path.of("test-database"))

  given Configuration = new Configuration:
    override def config: Config = Config("test-database", "", "", "")

  "write" should "should write the values to the database" in {
    createTable()
    val id = UUID.randomUUID
    val fileId = UUID.randomUUID()
    val file = ocflFile(id, fileId)
    Database[IO].write(List(file)).unsafeRunSync()
    val response = readFiles(id).unsafeRunSync()

    response.head should equal(file)
  }

  "write" should "should write the updated values to the database if a row already exists" in {
    createTable()
    val file = createFile().unsafeRunSync().copy(zref = "Another zref".some)

    Database[IO].write(List(file)).unsafeRunSync()
    val response = readFiles(file.id).unsafeRunSync()

    response.head.zref.get should equal("Another zref")
  }

  "write" should "write nothing if an empty list is passed" in {
    createTable()
    val id = UUID.randomUUID
    val fileId = UUID.randomUUID()
    val file = ocflFile(id, fileId)
    Database[IO].write(Nil).unsafeRunSync()
    val response = readFiles(id).unsafeRunSync()

    response.isEmpty should equal(true)
  }

  "write" should "return an error if there is an error with the database" in {
    val id = UUID.randomUUID
    val fileId = UUID.randomUUID()
    val file = ocflFile(id, fileId)

    val ex = intercept[Exception](Database[IO].write(Nil).unsafeRunSync())
    ex.getMessage should equal("[SQLITE_ERROR] SQL error or missing database (no such table: files)")
  }
