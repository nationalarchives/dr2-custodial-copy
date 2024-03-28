package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import io.ocfl.api.exception.NotFoundException
import io.ocfl.api.io.FixityCheckInputStream
import io.ocfl.api.{OcflFileRetriever, OcflObjectUpdater, OcflOption, OcflRepository}
import io.ocfl.api.model.{
  DigestAlgorithm,
  FileDetails,
  ObjectVersionId,
  OcflObjectVersion,
  OcflObjectVersionFile,
  VersionDetails,
  VersionInfo,
  VersionNum
}
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.gov.nationalarchives.DisasterRecoveryObject.FileObject
import uk.gov.nationalarchives.Main.IdWithPath

import java.io.ByteArrayInputStream
import java.nio.file.{Path, Paths}
import java.util.UUID
import java.util.function.Consumer
import scala.jdk.CollectionConverters.MapHasAsJava

class OcflServiceTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {

  private val url = "url"
  private val name = "name"
  val checksum = "checksum"

  val testOcflFileRetriever: OcflFileRetriever = () =>
    new FixityCheckInputStream(new ByteArrayInputStream("".getBytes), DigestAlgorithm.sha256, "checksum")

  def mockGetObjectResponse(
      ocflRepository: OcflRepository,
      id: UUID,
      ocflFileName: String,
      ocflChecksum: String
  ): Unit = {
    val fileDetails = new FileDetails()
    fileDetails.setFixity(Map(DigestAlgorithm.sha256 -> ocflChecksum).asJava)
    val ocflObjectVersionFile = new OcflObjectVersionFile(fileDetails, testOcflFileRetriever)
    val versionDetails = new VersionDetails()
    val v1 = VersionNum.V1
    versionDetails.setObjectVersionId(ObjectVersionId.version(id.toString, v1))
    val objectVersion = new OcflObjectVersion(versionDetails, Map(s"$id/$ocflFileName" -> ocflObjectVersionFile).asJava)
    when(ocflRepository.getObject(any[ObjectVersionId])).thenReturn(objectVersion)
  }

  "getMissingAndChangedObjects" should "return 1 missing object and 0 changed objects if the repository doesn't contain the object" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new NotFoundException)

    val service = new OcflService(ocflRepository)
    val fileObjectThatShouldBeMissing = FileObject(id, name, checksum, url)

    val missingAndChangedObjects =
      service.getMissingAndChangedObjects(List(fileObjectThatShouldBeMissing)).unsafeRunSync()
    val missingObject = missingAndChangedObjects.missingObjects.head

    missingAndChangedObjects.missingObjects.size should equal(1)
    missingObject should equal(fileObjectThatShouldBeMissing)

    missingAndChangedObjects.changedObjects.size should equal(0)
  }

  "getMissingAndChangedObjects" should "return 0 missing objects and 0 changed objects if the repository contains the object " +
    "but the checksums match" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, name, checksum)

      val service = new OcflService(ocflRepository)

      val missingAndChangedObjects =
        service.getMissingAndChangedObjects(List(FileObject(id, name, checksum, url))).unsafeRunSync()

      missingAndChangedObjects.missingObjects.size should equal(0)
      missingAndChangedObjects.changedObjects.size should equal(0)
    }

  "getMissingAndChangedObjects" should "return 0 missing objects and 1 changed objects if the repository contains the object " +
    "but the checksums don't match" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, name, checksum)

      val service = new OcflService(ocflRepository)
      val fileObjectThatShouldHaveChangedChecksum = FileObject(id, name, "anotherChecksum", url)

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
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new Exception("unexpected Exception"))

    val service = new OcflService(ocflRepository)
    val fileObjectThatShouldHaveChangedChecksum = FileObject(id, name, checksum, url)

    val ex = intercept[Exception] {
      service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"'getObject' returned an unexpected error 'java.lang.Exception: unexpected Exception' when called with object id $id"
    )
  }

  "createObjects" should "create objects in the OCFL repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val sourcePathCaptor: ArgumentCaptor[Path] = ArgumentCaptor.forClass(classOf[Path])
    val destinationPathCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val updater = mock[OcflObjectUpdater]
    when(updater.addPath(sourcePathCaptor.capture, destinationPathCaptor.capture, any[OcflOption]))
      .thenReturn(updater)

    when(ocflRepository.updateObject(objectVersionCaptor.capture, any[VersionInfo], any[Consumer[OcflObjectUpdater]]))
      .thenAnswer((_: ObjectVersionId, _: VersionInfo, consumer: Consumer[OcflObjectUpdater]) => {
        consumer.accept(updater)
        ObjectVersionId.head(id.toString)
      })
    val service = new OcflService(ocflRepository)

    service.createObjects(List(IdWithPath(id, Paths.get("test")))).unsafeRunSync()

    UUID.fromString(objectVersionCaptor.getValue.getObjectId) should equal(id)

    sourcePathCaptor.getValue.toString should equal("test")
    destinationPathCaptor.getValue should equal(s"$id/test")
  }
}
