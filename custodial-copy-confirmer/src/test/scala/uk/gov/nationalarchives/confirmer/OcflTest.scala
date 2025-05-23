package uk.gov.nationalarchives.confirmer

import io.ocfl.api.model.{ObjectVersionId, VersionInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.confirmer.Main.Config
import uk.gov.nationalarchives.utils.Utils.createOcflRepository

import java.net.URI
import java.nio.file.Files
import java.util.UUID

class OcflTest extends AnyFlatSpec:

  "checkObjectExists" should "return true if an object exists or false otherwise" in {
    val repoDir = Files.createTempDirectory("repo").toString
    val workDir = Files.createTempDirectory("work").toString
    val repository = createOcflRepository(repoDir, workDir)
    val existingRef = UUID.randomUUID
    val nonExistingRef = UUID.randomUUID
    repository.putObject(ObjectVersionId.head(existingRef.toString), Files.createTempFile(existingRef.toString, ""), new VersionInfo())

    val ocfl = Ocfl(Config("table", "attribute", "", URI.create("http://localhost"), repoDir, workDir, "", ""))
    ocfl.checkObjectExists(existingRef) should equal(true)
    ocfl.checkObjectExists(nonExistingRef) should equal(false)
  }
