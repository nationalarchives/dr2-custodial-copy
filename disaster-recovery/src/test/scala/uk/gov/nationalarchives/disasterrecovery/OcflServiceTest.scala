package uk.gov.nationalarchives.disasterrecovery

import cats.effect.unsafe.implicits.global
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.io.FixityCheckInputStream
import io.ocfl.api.model.*
import io.ocfl.api.{OcflFileRetriever, OcflObjectUpdater, OcflRepository}
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentCaptor
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito.{doNothing, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.gov.nationalarchives.disasterrecovery.DisasterRecoveryObject.FileObject
import uk.gov.nationalarchives.disasterrecovery.Main.IdWithSourceAndDestPaths

import java.io.ByteArrayInputStream
import java.nio.file.Paths
import java.util.UUID
import java.util.function.Consumer
import scala.jdk.CollectionConverters.MapHasAsJava

class OcflServiceTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {

  private val url = "url"
  private val name = "name"
  val checksum = "checksum"
  private val destinationPath = "destinationPath"

  val testOcflFileRetriever: OcflFileRetriever = () => new FixityCheckInputStream(new ByteArrayInputStream("".getBytes), DigestAlgorithm.sha256, "checksum")

  def mockGetObjectResponse(
      ocflRepository: OcflRepository,
      id: UUID,
      ocflChecksum: String,
      destinationPath: String
  ): Unit = {
    val fileDetails = new FileDetails()
    fileDetails.setFixity(Map(DigestAlgorithm.sha256 -> ocflChecksum).asJava)
    val ocflObjectVersionFile = new OcflObjectVersionFile(fileDetails, testOcflFileRetriever)
    val versionDetails = new VersionDetails()
    val v1 = VersionNum.V1
    versionDetails.setObjectVersionId(ObjectVersionId.version(id.toString, v1))
    val objectVersion = new OcflObjectVersion(versionDetails, Map(destinationPath -> ocflObjectVersionFile).asJava)
    when(ocflRepository.getObject(any[ObjectVersionId])).thenReturn(objectVersion)
  }

  "getMissingAndChangedObjects" should "return 1 missing DR object and 0 changed DR objects if the repository doesn't contain the OCFL object" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new NotFoundException)

    val service = new OcflService(ocflRepository)
    val fileObjectThatShouldBeMissing = FileObject(id, name, checksum, url, destinationPath)

    val missingAndChangedObjects =
      service.getMissingAndChangedObjects(List(fileObjectThatShouldBeMissing)).unsafeRunSync()
    val missingObject = missingAndChangedObjects.missingObjects.head

    missingAndChangedObjects.missingObjects.size should equal(1)
    missingObject should equal(fileObjectThatShouldBeMissing)

    missingAndChangedObjects.changedObjects.size should equal(0)
  }

  "getMissingAndChangedObjects" should "return 1 missing DR object and 0 changed DR objects if the repository contains the OCFL object " +
    "but doesn't contain the file" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksum, destinationPath)

      val service = new OcflService(ocflRepository)
      val fileObjectThatShouldBeMissing = FileObject(id, name, checksum, url, "nonExistingDestinationPath")

      val missingAndChangedObjects =
        service.getMissingAndChangedObjects(List(fileObjectThatShouldBeMissing)).unsafeRunSync()
      val missingObject = missingAndChangedObjects.missingObjects.head

      missingAndChangedObjects.missingObjects.size should equal(1)
      missingObject should equal(fileObjectThatShouldBeMissing)

      missingAndChangedObjects.changedObjects.size should equal(0)
    }

  "getMissingAndChangedObjects" should "return 0 missing DR objects and 0 changed DR objects if the repository contains the DR object " +
    "but the checksums match" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksum, destinationPath)

      val service = new OcflService(ocflRepository)

      val missingAndChangedObjects =
        service.getMissingAndChangedObjects(List(FileObject(id, name, checksum, url, destinationPath))).unsafeRunSync()

      missingAndChangedObjects.missingObjects.size should equal(0)
      missingAndChangedObjects.changedObjects.size should equal(0)
    }

  "getMissingAndChangedObjects" should "return 0 missing DR objects and 1 changed DR objects if the repository contains the DR object " +
    "but the checksums don't match" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksum, destinationPath)

      val service = new OcflService(ocflRepository)
      val fileObjectThatShouldHaveChangedChecksum = FileObject(id, name, "anotherChecksum", url, destinationPath)

      val missingAndChangedObjects =
        service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()

      missingAndChangedObjects.missingObjects.size should equal(0)
      missingAndChangedObjects.changedObjects.size should equal(1)
      val changedObject = missingAndChangedObjects.changedObjects.head
      changedObject should equal(fileObjectThatShouldHaveChangedChecksum)
    }

  "getMissingAndChangedObjects" should "throw an exception if 'ocflRepository.getObject' returns an unexpected Exception" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new RuntimeException("unexpected Exception"))

    val service = new OcflService(ocflRepository)
    val fileObjectThatShouldHaveChangedChecksum = FileObject(id, name, checksum, url, destinationPath)

    val ex = intercept[Exception] {
      service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"'getObject' returned an unexpected error 'java.lang.RuntimeException: unexpected Exception' when called with object id $id"
    )
  }

  "getMissingAndChangedObjects" should "purge the failed object if there is a CorruptedObjectException and rethrow the error" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new CorruptObjectException())
    doNothing().when(ocflRepository).purgeObject(objectIdCaptor.capture())

    val service = new OcflService(ocflRepository)
    val fileObjectThatShouldHaveChangedChecksum = FileObject(id, name, checksum, url, destinationPath)

    val ex = intercept[Exception] {
      service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"Object $id is corrupt. The object has been purged and the error will be rethrown so the process can try again"
    )
    objectIdCaptor.getValue should equal(id.toString)
  }

  "createObjects" should "create DR objects in the OCFL repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val updater = mock[OcflObjectUpdater]

    when(ocflRepository.updateObject(objectVersionCaptor.capture, any[VersionInfo], any[Consumer[OcflObjectUpdater]]))
      .thenAnswer(invocation => {
        val consumer = invocation.getArgument[Consumer[OcflObjectUpdater]](2)
        consumer.accept(updater)
        ObjectVersionId.head(id.toString)
      })
    val service = new OcflService(ocflRepository)

    service.createObjects(List(IdWithSourceAndDestPaths(id, Paths.get("test"), destinationPath))).unsafeRunSync()

    UUID.fromString(objectVersionCaptor.getValue.getObjectId) should equal(id)
  }
}
