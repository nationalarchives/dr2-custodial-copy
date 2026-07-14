package uk.gov.nationalarchives.confirmer

import io.ocfl.api.model.{ObjectVersionId, VersionInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.confirmer.Config
import uk.gov.nationalarchives.utils.Utils.createOcflRepository

import java.net.URI
import java.nio.file.Files
import java.util.UUID

class OcflTest extends AnyFlatSpec:

  "getFilePathsforObject" should "return valid paths if an object exists or empty list otherwise" in {
    val repoDir = Files.createTempDirectory("repo").toString
    val workDir = Files.createTempDirectory("work").toString
    val repository = createOcflRepository(repoDir, workDir)
    val existingRef = UUID.randomUUID
    val nonExistingRef = UUID.randomUUID
    val filePath = Files.createTempFile(existingRef.toString, "")
    repository.putObject(ObjectVersionId.head(existingRef.toString), filePath, new VersionInfo())

    val ocfl = Ocfl(Config("table", "attribute", "", Some(URI.create("http://localhost")), repoDir, workDir))
    ocfl.getFilePathsForObject(existingRef) should be(List(filePath.getFileName.toString))
    ocfl.getFilePathsForObject(nonExistingRef) should be(Nil)
  }
