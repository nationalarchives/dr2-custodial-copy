package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.io.FixityCheckInputStream
import io.ocfl.api.model.*
import io.ocfl.api.{MutableOcflRepository, OcflFileRetriever, OcflObjectUpdater, OcflOption}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, times, verify, verifyNoInteractions, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar
import uk.gov.nationalarchives.custodialcopy.CustodialCopyObject.FileObject
import uk.gov.nationalarchives.custodialcopy.Main.IdWithSourceAndDestPaths
import uk.gov.nationalarchives.dp.client.Entities.Entity

import java.io.{ByteArrayInputStream, InputStream}
import java.lang
import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.function.Consumer
import scala.jdk.CollectionConverters.*

class OcflServiceTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {

  private val url = "url"
  private val name = "name"
  val checksums = List(Checksum("MD5", "md5checksum"), Checksum("SHA256", "checksum"))
  private val destinationPath = "destinationPath"
  private val entity = mock[Entity]
  val semaphore: Semaphore[IO] = Semaphore[IO](1).unsafeRunSync()

  val testOcflFileRetriever: OcflFileRetriever = new OcflFileRetriever:
    override def retrieveFile(): FixityCheckInputStream =
      new FixityCheckInputStream(new ByteArrayInputStream("".getBytes), DigestAlgorithm.fromOcflName("sha256"), "checksum")

    override def retrieveRange(startPosition: lang.Long, endPosition: lang.Long): InputStream = new ByteArrayInputStream("".getBytes)

  def mockGetObjectResponse(
      ocflRepository: MutableOcflRepository,
      id: UUID,
      ocflChecksum: List[Checksum],
      destinationPath: String
  ): Unit = {
    val fileDetails = new FileDetails()
    fileDetails.setFixity(ocflChecksum.map(eachChecksum => DigestAlgorithm.fromOcflName(eachChecksum.algorithm) -> eachChecksum.fingerprint).toMap.asJava)
    fileDetails.setPath(destinationPath)
    val ocflObjectVersionFile = new OcflObjectVersionFile(fileDetails, testOcflFileRetriever)
    val versionDetails = new VersionDetails()
    val v1 = VersionNum.V1
    versionDetails.setObjectVersionId(ObjectVersionId.version(id.toString, v1))
    val objectVersion = new OcflObjectVersion(versionDetails, Map(destinationPath -> ocflObjectVersionFile).asJava)
    when(ocflRepository.getObject(any[ObjectVersionId])).thenReturn(objectVersion)
  }

  "getMissingAndChangedObjects" should "return 1 missing DR object and 0 changed DR objects if the repository doesn't contain the OCFL object" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new NotFoundException)

    val service = new OcflService(ocflRepository, semaphore)
    val fileObjectThatShouldBeMissing =
      FileObject(id, name, checksums, url, destinationPath, UUID.randomUUID)

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
      val ocflRepository = mock[MutableOcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksums, destinationPath)

      val service = new OcflService(ocflRepository, semaphore)
      val fileObjectThatShouldBeMissing =
        FileObject(id, name, checksums, url, "nonExistingDestinationPath", UUID.randomUUID)

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
      val ocflRepository = mock[MutableOcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksums, destinationPath)

      val service = new OcflService(ocflRepository, semaphore)

      val missingAndChangedObjects =
        service
          .getMissingAndChangedObjects(
            List(FileObject(id, name, checksums, url, destinationPath, UUID.randomUUID))
          )
          .unsafeRunSync()

      missingAndChangedObjects.missingObjects.size should equal(0)
      missingAndChangedObjects.changedObjects.size should equal(0)
    }

  "getMissingAndChangedObjects" should "return 0 missing DR objects and 1 changed DR objects if the repository contains the DR object " +
    "but the checksums don't match" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[MutableOcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksums, destinationPath)

      val service = new OcflService(ocflRepository, semaphore)
      val fileObjectThatShouldHaveChangedChecksum =
        FileObject(id, name, List(Checksum("SHA256", "anotherChecksum")), url, destinationPath, UUID.randomUUID)

      val missingAndChangedObjects =
        service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()

      missingAndChangedObjects.missingObjects.size should equal(0)
      missingAndChangedObjects.changedObjects.size should equal(1)
      val changedObject = missingAndChangedObjects.changedObjects.head
      changedObject should equal(fileObjectThatShouldHaveChangedChecksum)
    }

  "getMissingAndChangedObjects" should "throw an exception if 'ocflRepository.getObject' returns an unexpected Exception" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new RuntimeException("unexpected Exception"))

    val service = new OcflService(ocflRepository, semaphore)
    val fileObjectThatShouldHaveChangedChecksum =
      FileObject(id, name, checksums, url, destinationPath, UUID.randomUUID)

    val ex = intercept[Exception] {
      service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"'getObject' returned an unexpected error 'java.lang.RuntimeException: unexpected Exception' when called with object id $id"
    )
  }

  "getMissingAndChangedObjects" should "purge the failed object if there is a CorruptedObjectException and rethrow the error" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    val objectIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new CorruptObjectException())
    doNothing().when(ocflRepository).purgeObject(objectIdCaptor.capture())

    val service = new OcflService(ocflRepository, semaphore)
    val fileObjectThatShouldHaveChangedChecksum =
      FileObject(id, name, checksums, url, destinationPath, UUID.randomUUID)

    val ex = intercept[Exception] {
      service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"Object $id is corrupt. The object has been purged and the error will be rethrown so the process can try again"
    )
    objectIdCaptor.getValue should equal(id.toString)
  }

  "createObjects" should "not create an object if the file path doesn't exist" in {
    val ocflRepository = mock[MutableOcflRepository]
    val service = new OcflService(ocflRepository, semaphore)

    service.createObjects(List(IdWithSourceAndDestPaths(UUID.randomUUID, None, "destination"))).unsafeRunSync()

    verifyNoInteractions(ocflRepository)
  }

  "createObjects" should "create DR objects in the OCFL repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val updater = mock[OcflObjectUpdater]
    val sourceNioFilePathToAdd: ArgumentCaptor[Path] = ArgumentCaptor.forClass(classOf[Path])
    val destinationPathToAdd: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val optionMoveToAdd: ArgumentCaptor[OcflOption] = ArgumentCaptor.forClass(classOf[OcflOption])
    val optionOverwriteToAdd: ArgumentCaptor[OcflOption] = ArgumentCaptor.forClass(classOf[OcflOption])

    val inputPath = Files.createTempFile("ocfl", "test")

    when(ocflRepository.stageChanges(objectVersionCaptor.capture, any[VersionInfo], any[Consumer[OcflObjectUpdater]]))
      .thenAnswer { invocation =>
        val consumer = invocation.getArgument[Consumer[OcflObjectUpdater]](2)
        consumer.accept(updater)
        ObjectVersionId.head(id.toString)
      }
    val service = new OcflService(ocflRepository, semaphore)

    service.createObjects(List(IdWithSourceAndDestPaths(id, Option(inputPath), destinationPath))).unsafeRunSync()

    UUID.fromString(objectVersionCaptor.getValue.getObjectId) should equal(id)
    verify(updater, times(1)).addPath(
      sourceNioFilePathToAdd.capture(),
      destinationPathToAdd.capture,
      optionMoveToAdd.capture,
      optionOverwriteToAdd.capture()
    )
    sourceNioFilePathToAdd.getAllValues.asScala.toList should equal(List(inputPath))
    destinationPathToAdd.getAllValues.asScala.toList should equal(List(destinationPath))
    optionMoveToAdd.getAllValues.asScala.toList.head should equal(OcflOption.MOVE_SOURCE)
    optionOverwriteToAdd.getAllValues.asScala.toList.head should equal(OcflOption.OVERWRITE)
  }

  "getAllFilePathsOnAnObject" should "throw an exception if the object to be deleted does not exist" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new NotFoundException)

    val service = new OcflService(ocflRepository, semaphore)

    val err = intercept[Exception] {
      service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    }

    err.getMessage should equal(s"Object id $id does not exist")
  }

  "getAllFilePathsOnAnObject" should "return a path if the repository contains the OCFL object" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    mockGetObjectResponse(ocflRepository, id, checksums, destinationPath)

    val service = new OcflService(ocflRepository, semaphore)

    val filePathsOnAnObject = service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    filePathsOnAnObject.foreach(_ should equal(destinationPath))
  }

  "getAllFilePathsOnAnObject" should "throw an exception if 'ocflRepository.getObject' returns an unexpected Exception" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new RuntimeException("unexpected Exception"))

    val service = new OcflService(ocflRepository, semaphore)

    val ex = intercept[Exception] {
      service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"'getObject' returned an unexpected error 'java.lang.RuntimeException: unexpected Exception' when called with object id $id"
    )
  }

  "getAllFilePathsOnAnObject" should "purge the failed object if there is a CorruptedObjectException and rethrow the error" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    val objectIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new CorruptObjectException())
    doNothing().when(ocflRepository).purgeObject(objectIdCaptor.capture())

    val service = new OcflService(ocflRepository, semaphore)

    val ex = intercept[Exception] {
      service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"Object $id is corrupt. The object has been purged and the error will be rethrown so the process can try again"
    )
    objectIdCaptor.getValue should equal(id.toString)
  }

  "deleteObjects" should "make the call to delete CC objects in the OCFL repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[MutableOcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val updater = mock[OcflObjectUpdater]
    val pathsToDelete: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    when(ocflRepository.stageChanges(objectVersionCaptor.capture, any[VersionInfo], any[Consumer[OcflObjectUpdater]]))
      .thenAnswer { invocation =>
        val consumer = invocation.getArgument[Consumer[OcflObjectUpdater]](2)
        consumer.accept(updater)
        ObjectVersionId.head(id.toString)
      }
    val service = new OcflService(ocflRepository, semaphore)

    service.deleteObjects(id, List(destinationPath, "destinationPath2")).unsafeRunSync()

    UUID.fromString(objectVersionCaptor.getValue.getObjectId) should equal(id)
    verify(updater, times(2)).removeFile(pathsToDelete.capture)

    pathsToDelete.getAllValues.asScala.toList should equal(List(destinationPath, "destinationPath2"))
  }

  "commitStagedChanges" should "make a call to the repository commit if there are staged changes" in {
    val id = UUID.randomUUID
    val ocflRepository = mock[MutableOcflRepository]
    val commitIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val versionInfoCaptor: ArgumentCaptor[VersionInfo] = ArgumentCaptor.forClass(classOf[VersionInfo])
    val versionInfoReturnValue = new VersionInfo()
    versionInfoReturnValue.setUser("user", "address")
    versionInfoReturnValue.setMessage("message")
    val versionDetails = new VersionDetails()
    val v1 = VersionNum.V1
    versionDetails.setObjectVersionId(ObjectVersionId.version(id.toString, v1))
    versionDetails.setVersionInfo(versionInfoReturnValue)
    val objectVersion = new OcflObjectVersion(versionDetails, java.util.Map.of())

    when(ocflRepository.getObject(ArgumentMatchers.eq(ObjectVersionId.head(id.toString)))).thenReturn(objectVersion)

    when(ocflRepository.commitStagedChanges(commitIdCaptor.capture, versionInfoCaptor.capture())).thenReturn(ObjectVersionId.version(id.toString, 1))

    when(ocflRepository.hasStagedChanges(id.toString)).thenReturn(true)

    val service = new OcflService(ocflRepository, semaphore)

    service.commitStagedChanges(id).unsafeRunSync()

    commitIdCaptor.getValue should equal(id.toString)
    val capturedVersionInfo = versionInfoCaptor.getValue
    capturedVersionInfo should equal(null)
  }

  "commitStagedChanges" should "not commit to the repository if there are no staged changes" in {
    val id = UUID.randomUUID
    val ocflRepository = mock[MutableOcflRepository]
    when(ocflRepository.hasStagedChanges(id.toString)).thenReturn(false)

    new OcflService(ocflRepository, semaphore).commitStagedChanges(id).unsafeRunSync()

    verify(ocflRepository, times(0)).getObject(any[ObjectVersionId])
    verify(ocflRepository, times(0)).commitStagedChanges(any[String], any[VersionInfo])
  }
}
