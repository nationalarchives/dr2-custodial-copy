package uk.gov.nationalarchives.reindexer

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reindexer.Configuration.Config
import uk.gov.nationalarchives.utils.TestUtils.{coRef, coRefTwo, initialiseRepo}
import uk.gov.nationalarchives.utils.Utils.createOcflRepository

import java.nio.file.{Files, Path}
import java.util.UUID

class OcflSpec extends AnyFlatSpec with EitherValues:

  "allObjectsIds" should "return all object ids in the repository" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id)
    val repository = createOcflRepository(repoDir, workDir)
    case class IdPath(preservationId: UUID, path: Path)

    given Configuration = new Configuration:
      override def config: Config = Config("test-database", repoDir, workDir, 100)

    val allObjects = Ocfl[IO].allObjectsIds().compile.toList.unsafeRunSync()
    allObjects.length should equal(1)
    allObjects.head should equal(id)
  }

  "allObjectsIds" should "return an empty list for an empty repository" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = (Files.createTempDirectory("repo").toString, Files.createTempDirectory("work").toString)
    val repository = createOcflRepository(repoDir, workDir)
    case class IdPath(preservationId: UUID, path: Path)

    given Configuration = new Configuration:
      override def config: Config = Config("test-database", repoDir, workDir, 100)

    val allFiles = Ocfl[IO].allObjectsIds().compile.toList.unsafeRunSync()
    allFiles.length should equal(0)
  }

  "generateOcflObject" should "return all content object rows in the repository" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id)
    val repository = createOcflRepository(repoDir, workDir)
    case class IdPath(preservationId: UUID, path: Path)

    given Configuration = new Configuration:
      override def config: Config = Config("test-database", repoDir, workDir, 100)

    val ocflObjects = Ocfl[IO].generateOcflObject(id).unsafeRunSync()
    ocflObjects.count(_.fileId == coRef) should equal(1)
    ocflObjects.count(_.fileId == coRefTwo) should equal(1)
    ocflObjects.count(_.fileName.contains("Content Title")) should equal(1)
    ocflObjects.count(_.fileName.contains("Content Title2")) should equal(1)
  }

  "generateOcflObject" should "error if the id doesn't exist" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = (Files.createTempDirectory("repo").toString, Files.createTempDirectory("work").toString)
    val repository = createOcflRepository(repoDir, workDir)
    case class IdPath(preservationId: UUID, path: Path)

    given Configuration = new Configuration:
      override def config: Config = Config("test-database", repoDir, workDir, 100)

    val error = Ocfl[IO].generateOcflObject(id).attempt.unsafeRunSync().left.value
    error.getMessage should equal(s"Object ObjectId{objectId='$id', versionNum='null'} was not found.")
  }
