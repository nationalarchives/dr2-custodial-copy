package uk.gov.nationalarchives.confirmer

import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId, VersionInfo}
import io.ocfl.core.util.DigestUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
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
    val refHex = DigestUtil.computeDigestHex(DigestAlgorithm.fromOcflName("sha256"), existingRef.toString)
    val path = s"${refHex.slice(0, 3)}/${refHex.slice(3, 6)}/${refHex.slice(6, 9)}/$refHex/v1/content"
    val ocfl = Ocfl(CCConfig("table", "attribute", "", Some(URI.create("http://localhost")), repoDir, workDir))
    ocfl.getFilePathsForObject(existingRef) should be(List(s"$path/${filePath.getFileName}"))
    ocfl.getFilePathsForObject(nonExistingRef) should be(Nil)
  }
