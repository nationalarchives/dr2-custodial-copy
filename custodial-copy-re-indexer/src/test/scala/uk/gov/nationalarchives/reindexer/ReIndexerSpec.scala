package uk.gov.nationalarchives.reindexer

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import fs2.{Chunk, *}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.reindexer.Configuration.Config
import uk.gov.nationalarchives.utils.Utils
import uk.gov.nationalarchives.utils.Utils.OcflFile

import java.time.Instant
import java.util.UUID

class ReIndexerSpec extends AnyFlatSpec with EitherValues:
  val id: UUID = UUID.randomUUID
  val fileId: UUID = UUID.randomUUID
  val instant: Instant = Instant.now

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  given Configuration = new Configuration {
    override def config: Configuration.Config = Config("test-database", "repoDir", "workDir", 100)
  }

  def createOcflFile =
    OcflFile(
      1,
      id,
      "name".some,
      fileId,
      "ZREF".some,
      "path".some,
      "fileName".some,
      instant.some,
      "sourceId".some,
      "citation".some,
      "consignmentRef".some,
      "code".some
    )

  "reIndex" should "pass the correct values to the OCFL and Database methods" in {
    given Database[IO] = (files: Chunk[Utils.OcflFile]) =>
      files.toList.size should equal(1)
      files.head.get should equal(createOcflFile)
      IO(1)
    given Ocfl[IO] = new Ocfl[IO]:
      override def generateOcflObject(id: UUID): IO[List[OcflFile]] = IO.pure(List(createOcflFile))

      override def allObjectsIds(): Stream[IO, UUID] = Stream.emit(id)

    ReIndexer[IO].reIndex().unsafeRunSync()
  }

  "reIndex" should "chunk the updates to the write method" in {
    given Database[IO] = (files: Chunk[Utils.OcflFile]) =>
      files.toList.size should equal(100)
      files.foreach(file => file should equal(createOcflFile))
      IO(100)

    given Ocfl[IO] = new Ocfl[IO]:
      override def allObjectsIds(): Stream[IO, UUID] = Stream.emits(List.fill(200)(id))

      override def generateOcflObject(id: UUID): IO[List[OcflFile]] = IO.pure(List.fill(1)(createOcflFile))

    ReIndexer[IO].reIndex().unsafeRunSync()
  }

  "reIndex" should "not call the write method if there are no ids returned" in {
    given Database[IO] = (files: Chunk[Utils.OcflFile]) => IO.stub

    given Ocfl[IO] = new Ocfl[IO]:
      override def allObjectsIds(): fs2.Stream[IO, UUID] = Stream.empty

      override def generateOcflObject(id: UUID): IO[List[OcflFile]] = IO.pure(Nil)

    ReIndexer[IO].reIndex().unsafeRunSync()
  }

  "reIndex" should "raise an error if there is an error reading from OCFL" in {
    given Database[IO] = (files: Chunk[Utils.OcflFile]) => IO.stub

    given Ocfl[IO] = new Ocfl[IO]:
      override def allObjectsIds(): fs2.Stream[IO, UUID] =
        Stream.raiseError(new Exception("Error reading from OCFL"))

      override def generateOcflObject(id: UUID): IO[List[OcflFile]] = IO.stub

    val err = ReIndexer[IO].reIndex().attempt.map(_.left.value).unsafeRunSync().getMessage
    err should equal("Error reading from OCFL")
  }

  "reIndex" should "raise an error if there is an error writing to the database" in {
    given Database[IO] = (files: Chunk[Utils.OcflFile]) => IO.raiseError(new Exception("Error writing to the database"))

    given Ocfl[IO] = new Ocfl[IO]:
      override def allObjectsIds(): fs2.Stream[IO, UUID] =
        Stream.emit(id)

      override def generateOcflObject(id: UUID): IO[List[OcflFile]] = IO.pure(List(createOcflFile))

    val err = ReIndexer[IO].reIndex().attempt.map(_.left.value).unsafeRunSync().getMessage
    err should equal("Error writing to the database")
  }
