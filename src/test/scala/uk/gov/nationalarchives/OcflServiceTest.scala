package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import io.ocfl.api.io.FixityCheckInputStream
import io.ocfl.api.{OcflFileRetriever, OcflObjectUpdater, OcflRepository}
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
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
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

  "filterChangedObjects" should "not return the object if the repository doesn't contain the object" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.containsObject(ArgumentMatchers.eq(id.toString))).thenReturn(false)

    val service = new OcflService(ocflRepository)

    val changedObjects = service.filterChangedObjects(List(FileObject(id, name, "checksum", url)))
    changedObjects.size should equal(0)
  }

  "filterChangedObjects" should "not return an object if it is contained in the repository but the checksums match" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.containsObject(ArgumentMatchers.eq(id.toString))).thenReturn(true)
    mockGetObjectResponse(ocflRepository, id, name, checksum)

    val service = new OcflService(ocflRepository)

    val changedObjects = service.filterChangedObjects(List(FileObject(id, name, checksum, url)))
    changedObjects.size should equal(0)
  }

  "filterChangedObjects" should "return an object if it is contained in the repository and the checksums don't match" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.containsObject(ArgumentMatchers.eq(id.toString))).thenReturn(true)
    mockGetObjectResponse(ocflRepository, id, name, checksum)

    val service = new OcflService(ocflRepository)

    val changedObjects = service.filterChangedObjects(List(FileObject(id, name, "anotherChecksum", url)))
    changedObjects.size should equal(1)
  }

  "filterChangedObjects" should "not return an object if there is an object in the repository with a different " + name in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.containsObject(ArgumentMatchers.eq(id.toString))).thenReturn(true)
    mockGetObjectResponse(ocflRepository, id, name, checksum)

    val service = new OcflService(ocflRepository)

    val changedObjects = service.filterChangedObjects(List(FileObject(id, "anotherName", "anotherChecksum", url)))
    changedObjects.size should equal(0)
  }

  "filterMissingObjects" should "return an object if it is missing from the repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.containsObject(ArgumentMatchers.eq(id.toString))).thenReturn(false)

    val service = new OcflService(ocflRepository)

    val changedObjects = service.filterMissingObjects(List(FileObject(id, name, "checksum", url)))
    changedObjects.size should equal(1)
  }

  "filterMissingObjects" should "return an object if there is an object in the repository with a different " + name in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.containsObject(ArgumentMatchers.eq(id.toString))).thenReturn(true)
    mockGetObjectResponse(ocflRepository, id, name, "checksum")

    val service = new OcflService(ocflRepository)

    val changedObjects = service.filterMissingObjects(List(FileObject(id, "anotherName", "checksum", url)))
    changedObjects.size should equal(1)
  }

  "filterMissingObjects" should "not return an object if there is an object in the repository with the same " + name in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.containsObject(ArgumentMatchers.eq(id.toString))).thenReturn(true)
    mockGetObjectResponse(ocflRepository, id, name, "checksum")

    val service = new OcflService(ocflRepository)

    val changedObjects = service.filterMissingObjects(List(FileObject(id, name, "checksum", url)))
    changedObjects.size should equal(0)
  }

  "createObjects" should "create objects in the OCFL repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val sourcePathCaptor: ArgumentCaptor[Path] = ArgumentCaptor.forClass(classOf[Path])
    val destinationPathCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val updater = mock[OcflObjectUpdater]
    when(updater.addPath(sourcePathCaptor.capture, destinationPathCaptor.capture))
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

  "updateObjects" should "put a new version of an object in the repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val sourcePathCaptor: ArgumentCaptor[Path] = ArgumentCaptor.forClass(classOf[Path])
    when(ocflRepository.putObject(objectVersionCaptor.capture, sourcePathCaptor.capture, any[VersionInfo]))
      .thenReturn(ObjectVersionId.head(id.toString))

    val service = new OcflService(ocflRepository)

    service.updateObjects(List(IdWithPath(id, Paths.get("test")))).unsafeRunSync()

    UUID.fromString(objectVersionCaptor.getValue.getObjectId) should equal(id)
    sourcePathCaptor.getValue.toString should equal("test")
  }
}
