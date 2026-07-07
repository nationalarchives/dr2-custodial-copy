package uk.gov.nationalarchives.confirmer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*

class ConfirmerTest extends AnyFlatSpec {
  "Confirmer" should "return correct instance of confirmer based on the dynamo attribute in config" in {
    Confirmer.getConfirmer(Config("table", "CC_result", "", null, "", "")) should be(Confirmer.ccConfirmer)
    Confirmer.getConfirmer(Config("table", "TC_result", "", null, "", "")) should be(Confirmer.tcConfirmer)

    val ex = intercept[Exception] {
      Confirmer.getConfirmer(Config("table", "Unknown", "", null, "", ""))
    }
    ex.getMessage should equal(
      "Unable to create confirmer corresponding to Unknown"
    )
  }

  "CC Confirmer" should "return success when the payload and operator is valid" in {
    val uuid = java.util.UUID.randomUUID()
    val ccPayload = CCPayload(uuid)
    val ccOperator = CCOperator(TestUtils.ocfl(List(uuid), Config("table", "CC_result", "", null, "", "")))
    val ccResult = Confirmer.ccConfirmer.getResult(ccPayload, ccOperator)

    ccResult.isSuccess should be(true)
    ccResult match {
      case Confirmer.Result.Success(dynamoMap) =>
        dynamoMap("filePaths") should equal(List(s"/some/path/$uuid/file1.txt", s"/some/path/$uuid/file2.txt"))
      case _ => fail("Expected Success result for CC confirmer")
    }
  }

  "CC Confirmer" should "return failure when the payload is valid but the operator is invalid" in {
    val uuid = java.util.UUID.randomUUID()
    val ccPayload = CCPayload(uuid)
    val tcOperator = TCOperator(null) // Invalid operator for CC confirmer
    val ccResult = Confirmer.ccConfirmer.getResult(ccPayload, tcOperator)

    ccResult.isError should be(true)
    ccResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal(s"Unsupported operation in CC confirmer ${uuid.toString}")
      case _ => fail("Expected Failure result for CC confirmer with invalid operator")
    }
  }

  "CC Confirmer" should "return failure when the payload is invalid" in {
    val tcPayload = TCPayload(List("file1.txt", "file2.txt")) // Invalid payload for CC confirmer
    val ccOperator = CCOperator(TestUtils.ocfl(List(java.util.UUID.randomUUID()), Config("table", "CC_result", "", null, "", "")))
    val ccResult = Confirmer.ccConfirmer.getResult(tcPayload, ccOperator)

    ccResult.isError should be(true)
    ccResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Invalid payload type for CC confirmer")
      case _ => fail("Expected Failure result for CC confirmer with invalid payload")
    }
  }

  "TC Confirmer" should "return success when the payload and operator is valid" in {
    val filePaths = List("file1.txt", "file2.txt")
    val tcPayload = TCPayload(filePaths)
    val tcOperator = TCOperator(TestUtils.scoutAM(filePaths, Config("table", "TC_result", "", null, "", "")))
    val tcResult = Confirmer.tcConfirmer.getResult(tcPayload, tcOperator)

    tcResult.isSuccess should be(true)
    tcResult match {
      case Confirmer.Result.Success(dynamoMap) =>
        dynamoMap("file1.txt") should equal(List("Volume1file1.txt", "Volume2file1.txt"))
        dynamoMap("file2.txt") should equal(List("Volume1file2.txt", "Volume2file2.txt"))
      case _ => fail("Expected Success result for TC confirmer")
    }
  }

  "TC Confirmer" should "return failure when the payload is valid but the operator is invalid" in {
    val filePaths = List("file1.txt", "file2.txt")
    val tcPayload = TCPayload(filePaths)
    val ccOperator =
      CCOperator(TestUtils.ocfl(List(java.util.UUID.randomUUID()), Config("table", "CC_result", "", null, "", ""))) // Invalid operator for TC confirmer
    val tcResult = Confirmer.tcConfirmer.getResult(tcPayload, ccOperator)

    tcResult.isError should be(true)
    tcResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Unsupported operation in TC confirmer")
      case _ => fail("Expected Failure result for TC confirmer with invalid operator")
    }
  }

  "TC Confirmer" should "return failure when the payload is invalid" in {
    val ccPayload = CCPayload(java.util.UUID.randomUUID()) // Invalid payload for TC confirmer
    val tcOperator = TCOperator(TestUtils.scoutAM(List("file1.txt", "file2.txt"), Config("table", "TC_result", "", null, "", "")))
    val tcResult = Confirmer.tcConfirmer.getResult(ccPayload, tcOperator)

    tcResult.isError should be(true)
    tcResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Invalid payload type for TC confirmer")
      case _ => fail("Expected Failure result for TC confirmer with invalid payload")
    }
  }
}
