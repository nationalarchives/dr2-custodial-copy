package uk.gov.nationalarchives.disasterrecovery

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.Path
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.disasterrecovery.DisasterRecoveryObject.MetadataObject
import uk.gov.nationalarchives.disasterrecovery.Main.IdWithSourceAndDestPaths
import uk.gov.nationalarchives.disasterrecovery.Message.InformationObjectMessage
import uk.gov.nationalarchives.disasterrecovery.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives.disasterrecovery.Processor.{CoSnsMessage, DependenciesForIoSnsMsg, IoSnsMessage}
import uk.gov.nationalarchives.disasterrecovery.Processor.ObjectStatus.{Created, Updated}
import uk.gov.nationalarchives.disasterrecovery.Processor.ObjectType.Metadata
import uk.gov.nationalarchives.dp.client.EntityClient.IoMetadata
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.disasterrecovery.testUtils.ExternalServicesTestUtils.*

import java.util.UUID
import scala.xml.Elem

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

  "process" should "retrieve the bitstream info for a CO message" in {
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
      xmlRequestsToValidate = List(utils.coXmlToValidate),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      drosToLookup = List(
        List(
          s"$parentRef/Preservation_1/$id/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          s"$parentRef/Preservation_1/$id/CO_Metadata.xml"
        )
      ),
      receiptHandles = List("receiptHandle2", "receiptHandle2", "receiptHandle2")
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
      xmlRequestsToValidate = Nil,
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
      xmlRequestsToValidate = Nil,
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
      xmlRequestsToValidate = List(utils.ioXmlToValidate, utils.coXmlToValidate),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      drosToLookup = List(
        List(
          s"$parentRef/IO_Metadata.xml",
          s"$parentRef/Preservation_1/$id/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          s"$parentRef/Preservation_1/$id/CO_Metadata.xml"
        )
      ),
      receiptHandles = List(1, 1, 1, 2, 2, 2).map(n => s"receiptHandle$n")
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
            List(
              MetadataObject(
                id,
                Some("Preservation_1"),
                "changed",
                "checksum",
                utils.ioConsolidatedMetadata,
                "destinationPath",
                DependenciesForIoSnsMsg(
                  Entity(
                    Some(InformationObject),
                    id,
                    None,
                    None,
                    deleted = false,
                    None,
                    parent = Some(UUID.randomUUID())
                  ),
                  Seq(
                    <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>,
                    <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
                  )
                )
              )
            )
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
      ),
      snsMessagesToSend = List(
        IoSnsMessage(id, Metadata, Updated, "SourceIDValue")
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
            List(
              MetadataObject(
                id,
                Some("Preservation_1"),
                "missing",
                "checksum",
                utils.ioConsolidatedMetadata,
                "destinationPath",
                DependenciesForIoSnsMsg(
                  Entity(
                    Some(InformationObject),
                    id,
                    None,
                    None,
                    deleted = false,
                    None,
                    parent = Some(UUID.randomUUID())
                  ),
                  Seq(
                    <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>,
                    <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
                  )
                )
              )
            ),
            Nil
          )
        )
      )

    utils.processor.process(utils.duplicatesIoMessageResponses).unsafeRunSync()

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId =
        List(List(IdWithSourceAndDestPaths(id, Path(s"$id/missing").toNioPath, "destinationPath")), List()),
      snsMessagesToSend = List(
        IoSnsMessage(id, Metadata, Created, "SourceIDValue")
      )
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
                utils.ioConsolidatedMetadata,
                "destinationPath",
                DependenciesForIoSnsMsg(
                  Entity(
                    Some(InformationObject),
                    missingFileId,
                    None,
                    None,
                    deleted = false,
                    None,
                    parent = Some(UUID.randomUUID())
                  ),
                  Seq(
                    <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>,
                    <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
                  )
                )
              )
            ),
            List(
              MetadataObject(
                changedFileId,
                Some("Preservation_1"),
                "changed",
                "checksum",
                utils.ioConsolidatedMetadata,
                "destinationPath2",
                DependenciesForIoSnsMsg(
                  Entity(
                    Some(InformationObject),
                    changedFileId,
                    None,
                    None,
                    deleted = false,
                    None,
                    parent = Some(UUID.randomUUID())
                  ),
                  Seq(
                    <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>,
                    <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
                  )
                )
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
      xmlRequestsToValidate = List(utils.ioXmlToValidate, utils.ioXmlToValidate),
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
      snsMessagesToSend = List(
        IoSnsMessage(missingFileId, Metadata, Created, "SourceIDValue"),
        IoSnsMessage(changedFileId, Metadata, Updated, "SourceIDValue")
      ),
      receiptHandles = List("receiptHandle1", "receiptHandle2")
    )
  }

  "process" should "throw an Exception if XML string passed to validator is invalid" in {
    val utils = new ProcessorTestUtils()

    doReturn(
      IO.pure(
        IoMetadata(
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><InvalidTag/></InformationObject>,
          Seq(
            <Representation><InformationObject/><Name/><Type/><ContentObjects/><RepresentationFormats/><RepresentationProperties/></Representation>
          ),
          Seq(<Identifier><ApiId/><Type/><Value/></Identifier>),
          Seq(<Links><Link/></Links>),
          Seq(<Metadata><Content/></Metadata>),
          Seq(
            <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date/><User/></Event><Date></Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity></EventAction>
          )
        )
      )
    ).when(utils.entityClient).metadataForEntity(ArgumentMatchers.argThat(new EntityWithSpecificType("IO")))

    val res = utils.processor.process(utils.duplicatesIoMessageResponses).attempt.unsafeRunSync()

    res.left.foreach(
      _.getMessage should equal(
        """cvc-complex-type.2.4.a: Invalid content was found starting with element '{"http://preservica.com/XIP/v7.0":InvalidTag}'. """ +
          """One of '{"http://preservica.com/XIP/v7.0":Parent}' is expected."""
      )
    )

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = Nil,
      xmlRequestsToValidate = List(
        <XIP xmlns="http://preservica.com/XIP/v7.0">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><InvalidTag/></InformationObject>
          <Representation><InformationObject/><Name/><Type/><ContentObjects/><RepresentationFormats/><RepresentationProperties/></Representation>
          <Identifier><ApiId/><Type/><Value/></Identifier>
          <Links><Link/></Links>
          <Metadata><Content/></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date/><User/></Event><Date/><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity></EventAction>
        </XIP>
      ),
      drosToLookup = Nil,
      receiptHandles = Nil
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
