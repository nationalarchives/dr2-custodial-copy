package uk.gov.nationalarchives.reindexer

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.utils.TestUtils.*
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reindexer.Configuration.Config

import java.nio.file.{Files, Path}
import java.util.UUID

class DatabaseSpec extends AnyFlatSpec with BeforeAndAfterEach:

  val databaseUtils = new DatabaseUtils("test-reindexer-database")
  import databaseUtils.*

  override def afterEach(): Unit = Files.delete(Path.of("test-reindexer-database"))

  override def beforeEach(): Unit = createFilesTable()

  given Configuration = new Configuration:
    override def config: Configuration.Config = Config("test-reindexer-database", "", "", 100)

  "write" should "should write the values to the database" in {
    val id = UUID.randomUUID
    val fileId = UUID.randomUUID()
    val initialResponse = readFiles(id).unsafeRunSync()
    val file = ocflFile(id, fileId)
    Database[IO].write(Chunk(file)).unsafeRunSync()
    val response = readFiles(id).unsafeRunSync()

    initialResponse.isEmpty should equal(true)
    response.head should equal(file)
  }

  "write" should "write nothing if an empty list is passed" in {
    val id = UUID.randomUUID
    val fileId = UUID.randomUUID()
    val file = ocflFile(id, fileId)
    Database[IO].write(Chunk.empty).unsafeRunSync()
    val response = readFiles(id).unsafeRunSync()

    response.isEmpty should equal(true)
  }
