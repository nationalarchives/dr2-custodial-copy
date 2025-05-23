package uk.gov.nationalarchives.confirmer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.confirmer.Main.Message
import uk.gov.nationalarchives.confirmer.TestUtils.*

import java.util.UUID

class MainTest extends AnyFlatSpec {

  "runConfirmer" should "write to dynamo with the source id and delete the messages from the queue only if the object exists" in {
    val existingRef = UUID.randomUUID
    val existingSourceId = UUID.randomUUID
    val nonExistingRef = UUID.randomUUID
    val nonExistingSourceId = UUID.randomUUID()
    val assetIdToEntityIds = Map(existingSourceId -> List(existingRef), nonExistingSourceId -> List(nonExistingRef))
    val inputMessages = List(
      Message(existingSourceId, "batchId1"),
      Message(nonExistingSourceId, "batchId2")
    ).map(message => MessageResponse(message.batchId, None, message))
    val (deletedMessages, dynamoUpdateItems) = runConfirmer(inputMessages, List(existingRef), assetIdToEntityIds, Errors())

    deletedMessages.size should equal(1)
    deletedMessages.sorted should equal(List("batchId1"))
    dynamoUpdateItems.size should equal(1)

    val updateItem = dynamoUpdateItems.head
    updateItem.primaryKeyAndItsValue("assetId").s() should equal(existingSourceId.toString)
    updateItem.primaryKeyAndItsValue("batchId").s() should equal("batchId1")
    updateItem.attributeNamesAndValuesToUpdate("attribute").get.s() should equal("true")
  }

  "runConfirmer" should "not delete the messages from the queue if there is an error" in {
    val existingRef = UUID.randomUUID()
    val existingAssetId = UUID.randomUUID()
    val assetIdToEntityIds = Map(existingAssetId -> List(existingRef))
    val inputMessages = List(MessageResponse("batchId", None, Message(existingRef, "batchId1")))
    val (messagesInQueueOne, _) = runConfirmer(inputMessages, List(existingRef), assetIdToEntityIds, Errors(dynamoUpdateError = true))
    val (messagesInQueueTwo, _) = runConfirmer(inputMessages, List(existingRef), assetIdToEntityIds, Errors(sqsReceiveError = true))
    val (messagesInQueueThree, _) = runConfirmer(inputMessages, List(existingRef), assetIdToEntityIds, Errors(sqsDeleteError = true))

    messagesInQueueOne.size should equal(0)
    messagesInQueueTwo.size should equal(0)
    messagesInQueueThree.size should equal(0)
  }

  "runConfirmer" should "process up to 50 messages if more are available" in {
    val existingRef = UUID.randomUUID()
    val existingAssetId = UUID.randomUUID()
    val assetIdToEntityIds = Map(existingAssetId -> List(existingRef))
    val inputMessages = List(MessageResponse("batchId", None, Message(existingAssetId, "batchId1")))
    val (_, dynamoUpdates) = runConfirmer(inputMessages, List(existingRef), assetIdToEntityIds, Errors(), true)

    dynamoUpdates.size should equal(50)
  }

  "runConfirmer" should "not write to Dynamo or delete messages if an entity cannot be found with the source id" in {
    val existingRef = UUID.randomUUID()
    val existingAssetId = UUID.randomUUID()
    val assetIdToEntityIds = Map(existingAssetId -> Nil)
    val inputMessages = List(MessageResponse("batchId", None, Message(existingAssetId, "batchId1")))
    val (deletedMessages, dynamoUpdates) = runConfirmer(inputMessages, List(existingRef), assetIdToEntityIds, Errors())

    deletedMessages.size should equal(0)
    dynamoUpdates.size should equal(0)
  }

  "runConfirmer" should "not write to Dynamo or delete messages if more than one entity is found with the source id" in {
    val existingRef = UUID.randomUUID()
    val existingAssetId = UUID.randomUUID()
    val assetIdToEntityIds = Map(existingAssetId -> List(existingAssetId, UUID.randomUUID()))
    val inputMessages = List(MessageResponse("batchId", None, Message(existingAssetId, "batchId1")))
    val (deletedMessages, dynamoUpdates) = runConfirmer(inputMessages, List(existingRef), assetIdToEntityIds, Errors())

    deletedMessages.size should equal(0)
    dynamoUpdates.size should equal(0)
  }
}
