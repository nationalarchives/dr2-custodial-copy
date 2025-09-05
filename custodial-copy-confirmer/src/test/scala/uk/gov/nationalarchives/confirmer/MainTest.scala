package uk.gov.nationalarchives.confirmer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.confirmer.Main.{OutputQueueMessage, Payload}
import uk.gov.nationalarchives.confirmer.TestUtils.*

import java.util.UUID

class MainTest extends AnyFlatSpec {

  "runConfirmer" should "write to dynamo and delete the messages from the queue only if the object exists" in {
    val existingAssetId = UUID.randomUUID
    val existingRef = UUID.randomUUID
    val nonExistingRef = UUID.randomUUID
    val inputMessages = List(
      OutputQueueMessage(existingAssetId, "batchId1", Payload(existingRef)),
      OutputQueueMessage(UUID.randomUUID, "batchId2", Payload(nonExistingRef))
    ).map(message => MessageResponse(message.batchId, None, message))
    val (deletedMessages, dynamoUpdateItems) = runConfirmer(inputMessages, List(existingRef), Errors())

    deletedMessages.size should equal(1)
    deletedMessages.sorted should equal(List("batchId1"))
    dynamoUpdateItems.size should equal(1)

    val updateItem = dynamoUpdateItems.head
    updateItem.primaryKeyAndItsValue("assetId").s() should equal(existingAssetId.toString)
    updateItem.primaryKeyAndItsValue("batchId").s() should equal("batchId1")
    updateItem.attributeNamesAndValuesToUpdate("attribute").s() should equal("true")
  }

  "runConfirmer" should "not delete the messages from the queue if there is an error" in {
    val existingRef = UUID.randomUUID()
    val inputMessages = List(MessageResponse("batchId", None, OutputQueueMessage(UUID.randomUUID, "batchId1", Payload(existingRef))))
    val (messagesInQueueOne, _) = runConfirmer(inputMessages, List(existingRef), Errors(dynamoUpdateError = true))
    val (messagesInQueueTwo, _) = runConfirmer(inputMessages, List(existingRef), Errors(sqsReceiveError = true))
    val (messagesInQueueThree, _) = runConfirmer(inputMessages, List(existingRef), Errors(sqsDeleteError = true))

    messagesInQueueOne.size should equal(0)
    messagesInQueueTwo.size should equal(0)
    messagesInQueueThree.size should equal(0)
  }

  "runConfirmer" should "process up to 50 messages if more are available" in {
    val existingRef = UUID.randomUUID()
    val inputMessages = List(MessageResponse("batchId", None, OutputQueueMessage(UUID.randomUUID, "batchId1", Payload(existingRef))))
    val (_, dynamoUpdates) = runConfirmer(inputMessages, List(existingRef), Errors(), true)

    dynamoUpdates.size should equal(50)
  }
}
