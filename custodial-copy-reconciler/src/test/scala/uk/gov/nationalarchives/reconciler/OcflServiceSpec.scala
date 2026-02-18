package uk.gov.nationalarchives.reconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.ocfl.api.model.{ObjectVersionId, VersionInfo}
import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reconciler.Main.Config
import uk.gov.nationalarchives.utils.Utils.*

import java.nio.file.{Files, Path}
import java.util.UUID

class OcflServiceSpec extends AnyFlatSpec {

  "getAllObjectFiles" should "return all files in the repository" in {
    val (repoDir, workDir) = (Files.createTempDirectory("repo").toString, Files.createTempDirectory("work").toString)
    val repository = createOcflRepository(repoDir, workDir)
    val id = UUID.randomUUID

    case class IdPath(preservationId: UUID, path: Path)

    val idPaths = List.fill(100)(UUID.randomUUID).map { preservationId =>
      val preservationTestFile = Files.createTempFile(preservationId.toString, "file")
      val path = Files.write(preservationTestFile, preservationId.toString.getBytes)
      IdPath(preservationId, path)
    }
    repository.updateObject(
      ObjectVersionId.head(id.toString),
      new VersionInfo(),
      updater =>
        idPaths.map { idPath =>
          updater
            .addPath(idPath.path, s"$id/Preservation_1/${idPath.preservationId}")
        }
    )

    val config = Config("", "test-database", 1, repoDir, workDir, 0)
    val allFiles = OcflService[IO](config).getAllObjectFiles.compile.toList.unsafeRunSync()
    allFiles.length should equal(100)

    idPaths.map(_.preservationId).map { preservationId =>
      val file = allFiles.find(_.id == preservationId).get
      file.parent.get should equal(id)
      file.sha256Checksum should equal(DigestUtils.sha256Hex(preservationId.toString))
    }
  }

  "getAllObjectFiles" should "return 'Preservation' and 'Access' copies" in {
    val (repoDir, workDir) = (Files.createTempDirectory("repo").toString, Files.createTempDirectory("work").toString)
    val repository = createOcflRepository(repoDir, workDir)
    val preservationId = UUID.randomUUID
    val accessId = UUID.randomUUID
    val preservationTestFile = Files.createTempFile("test_ps", "file")
    Files.write(preservationTestFile, preservationId.toString.getBytes)
    val accessTestFile = Files.createTempFile("test_access", "file")
    Files.write(accessTestFile, accessId.toString.getBytes)
    val id = UUID.randomUUID

    repository.updateObject(
      ObjectVersionId.head(id.toString),
      new VersionInfo(),
      updater =>
        updater
          .addPath(preservationTestFile, s"$id/Preservation_1/$preservationId")
          .addPath(accessTestFile, s"$id/Access_1/$accessId")
    )
    val config = Config("", "test-database", 1, repoDir, workDir, 0)
    val allFiles = OcflService[IO](config).getAllObjectFiles.compile.toList.unsafeRunSync()
    allFiles.length should equal(2)

    allFiles.contains(CoRow(preservationId, Option(id), DigestUtils.sha256Hex(preservationId.toString))) should equal(true)
    allFiles.contains(CoRow(accessId, Option(id), DigestUtils.sha256Hex(accessId.toString))) should equal(true)

  }
}
