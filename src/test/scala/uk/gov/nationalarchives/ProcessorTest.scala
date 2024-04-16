package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.Path
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.DisasterRecoveryObject.MetadataObject
import uk.gov.nationalarchives.Main.IdWithSourceAndDestPaths
import uk.gov.nationalarchives.Message.InformationObjectMessage
import uk.gov.nationalarchives.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils.*

import java.util.UUID

class ProcessorTest extends AnyFlatSpec with MockitoSugar {
  "process" should "retrieve the metadata if an information object update is received" in {
    val utils = new ProcessorTestUtils()

    utils.processor.process(utils.duplicatesIoMessageResponses).unsafeRunSync()
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil)
    )
  }

  "process" should "retrieve the bitstream info for a CO update message" in {
    val utils = new ProcessorTestUtils()
    val id = utils.coId
    val parentRef = utils.ioId

    utils.processor.process(utils.duplicatesCoMessageResponses).unsafeRunSync()

    utils.verifyCallsAndArguments(
      1,
      1,
      1,
      idsOfEntityToGetMetadataFrom = List(id),
      entityTypesToGetMetadataFrom = List(ContentObject),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      drosToLookup = List(
        List(
          s"$parentRef/Preservation_1/$id/original/g1/name",
          s"$parentRef/Preservation_1/$id/CO_Metadata.xml"
        )
      )
    )
  }

  "process" should "retrieve the metadata if a content object update is received" in {
    val utils = new ProcessorTestUtils()
    val id = utils.coId
    val parentRef = utils.ioId

    utils.processor.process(utils.duplicatesCoMessageResponses).unsafeRunSync()
    utils.verifyCallsAndArguments(
      1,
      1,
      1,
      idsOfEntityToGetMetadataFrom = List(utils.coId),
      entityTypesToGetMetadataFrom = List(ContentObject),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      drosToLookup = List(
        List(
          s"$parentRef/Preservation_1/$id/original/g1/name",
          s"$parentRef/Preservation_1/$id/CO_Metadata.xml"
        )
      )
    )
  }

  "process" should "throw an Exception if a Content Object does not have a parent" in {
    val utils = new ProcessorTestUtils(parentRefExists = false)

    val ex = intercept[Exception] {
      utils.processor.process(utils.duplicatesCoMessageResponses).unsafeRunSync()
    }

    ex.getMessage should equal("Cannot get IO reference from CO")
    utils.verifyCallsAndArguments(
      1,
      repTypes = Nil,
      repIndexes = Nil,
      idsOfEntityToGetMetadataFrom = Nil,
      entityTypesToGetMetadataFrom = Nil,
      createdIdSourceAndDestinationPathAndId = Nil,
      drosToLookup = Nil,
      receiptHandles = Nil
    )
  }

  "process" should "throw an Exception if a Content Object belongs to more than 1 Representation type" in {
    val utils = new ProcessorTestUtils(urlsToRepresentations =
      Seq(
        "http://testurl/representations/Preservation/1",
        "http://testurl/representations/Preservation/2",
        "http://testurl/representations/Access/1"
      )
    )
    val id = utils.coId

    val ex = intercept[Exception] {
      utils.processor.process(utils.duplicatesCoMessageResponses).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"$id belongs to more than 1 representation type: Preservation_1, Preservation_2, Access_1"
    )
    utils.verifyCallsAndArguments(
      1,
      1,
      3,
      repTypes = List(Preservation, Preservation, Access),
      repIndexes = List(1, 2, 1),
      idsOfEntityToGetMetadataFrom = Nil,
      entityTypesToGetMetadataFrom = Nil,
      drosToLookup = Nil,
      receiptHandles = Nil
    )
  }

  "process" should "generate the correct destination paths for IO/CO metadata and CO bitstreams" in {
    val utils = new ProcessorTestUtils()
    val id = utils.coId
    val parentRef = utils.ioId

    utils.processor.process(utils.duplicatesIoMessageResponses ++ utils.duplicatesCoMessageResponses).unsafeRunSync()
    utils.verifyCallsAndArguments(
      1,
      1,
      1,
      idsOfEntityToGetMetadataFrom = List(parentRef, id),
      entityTypesToGetMetadataFrom = List(InformationObject, ContentObject),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      drosToLookup = List(
        List(
          s"$parentRef/IO_Metadata.xml",
          s"$parentRef/Preservation_1/$id/original/g1/name",
          s"$parentRef/Preservation_1/$id/CO_Metadata.xml"
        )
      ),
      receiptHandles = (1 to 6).toList.map(_ => "receiptHandle")
    )
  }

  "process" should "not create nor update objects if no objects are missing or changed" in {
    val utils = new ProcessorTestUtils()

    utils.processor.process(utils.duplicatesIoMessageResponses).unsafeRunSync()
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil)
    )
  }

  "process" should "update but not create if the object has changed" in {
    val utils = new ProcessorTestUtils()
    val id = utils.ioId

    when(utils.ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(
        IO.pure(
          MissingAndChangedObjects(
            Nil,
            List(MetadataObject(id, Some("Preservation_1"), "changed", "checksum", utils.metadata, "destinationPath"))
          )
        )
      )

    utils.processor.process(utils.duplicatesIoMessageResponses).unsafeRunSync()
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = List(
        List(), // 1st call to 'createObjects' with missingObjectsPaths arg
        List(
          IdWithSourceAndDestPaths(id, Path(s"$id/changed").toNioPath, "destinationPath")
        ) // 2nd call to 'createObjects' with changedObjectsPaths arg
      )
    )
  }

  "process" should "create but not update if the object is missing" in {
    val utils = new ProcessorTestUtils()
    val id = utils.ioId

    when(utils.ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(
        IO.pure(
          MissingAndChangedObjects(
            List(MetadataObject(id, Some("Preservation_1"), "missing", "checksum", utils.metadata, "destinationPath")),
            Nil
          )
        )
      )

    utils.processor.process(utils.duplicatesIoMessageResponses).unsafeRunSync()

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId =
        List(List(IdWithSourceAndDestPaths(id, Path(s"$id/missing").toNioPath, "destinationPath")), List())
    )
  }

  "process" should "update a changed file and add a missing file" in {
    val utils = new ProcessorTestUtils()
    val missingFileId = UUID.randomUUID()
    val changedFileId = utils.ioId

    when(utils.ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(
        IO.pure(
          MissingAndChangedObjects(
            List(
              MetadataObject(
                missingFileId,
                Some("Preservation_1"),
                "missing",
                "checksum",
                utils.metadata,
                "destinationPath"
              )
            ),
            List(
              MetadataObject(
                changedFileId,
                Some("Preservation_1"),
                "changed",
                "checksum",
                utils.metadata,
                "destinationPath2"
              )
            )
          )
        )
      )

    val responses: List[MessageResponse[Option[Message]]] = List(
      MessageResponse[Option[Message]](
        "receiptHandle1",
        Option(InformationObjectMessage(missingFileId, s"io:$missingFileId"))
      ),
      MessageResponse[Option[Message]](
        "receiptHandle2",
        Option(InformationObjectMessage(changedFileId, s"io:$changedFileId"))
      )
    )
    utils.processor.process(responses).unsafeRunSync()

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      idsOfEntityToGetMetadataFrom = List(missingFileId, changedFileId),
      createdIdSourceAndDestinationPathAndId = List(
        List(IdWithSourceAndDestPaths(missingFileId, Path(s"$missingFileId/missing").toNioPath, "destinationPath")),
        List(IdWithSourceAndDestPaths(changedFileId, Path(s"$changedFileId/changed").toNioPath, "destinationPath2"))
      ),
      drosToLookup = List(
        List(
          s"$missingFileId/IO_Metadata.xml",
          s"$changedFileId/IO_Metadata.xml"
        )
      ),
      receiptHandles = List("receiptHandle1", "receiptHandle2")
    )
  }

  "process" should "throw an Exception if an Unexpected Exception was returned from the OCFL service" in {
    val utils = new ProcessorTestUtils(throwErrorInMissingAndChangedObjects = true)

    val res = utils.processor.process(utils.duplicatesIoMessageResponses).attempt.unsafeRunSync()

    res.left.foreach(_.getMessage should equal("Unexpected Error"))

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      receiptHandles = Nil
    )
  }
}
