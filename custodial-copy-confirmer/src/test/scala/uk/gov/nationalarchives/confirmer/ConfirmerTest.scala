package uk.gov.nationalarchives.confirmer

import uk.gov.nationalarchives.confirmer.ResultAttributeName.{RESULT_CC, RESULT_TC}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.confirmer.TestUtils.{OcflErrors, ScoutAmErrors}

class ConfirmerTest extends AnyFlatSpec {
  "getConfirmer" should "return correct instance of confirmer based on the dynamo attribute in config" in {
    Confirmer.getConfirmer(Config("table", RESULT_CC.toString, "", null, "", "")) should be(Confirmer.ccConfirmer)
    Confirmer.getConfirmer(Config("table", RESULT_TC.toString, "", null, "", "")) should be(Confirmer.tcConfirmer)
  }

  "getConfirmer" should "throw an exception when an unknown dynamo attribute is provided in config" in {
    val ex = intercept[Exception] {
      Confirmer.getConfirmer(Config("table", "Unknown", "", null, "", ""))
    }
    ex.getMessage should equal(
      "Unknown ResultAttributeName: Unknown"
    )
  }

  "CC Confirmer getResult" should "return success when the payload and service is valid" in {
    val uuid = java.util.UUID.randomUUID()
    val ccPayload = CCPayload(uuid)
    val ccService = CCService(TestUtils.ocfl(List(uuid), Config("table", RESULT_CC.toString, "", null, "", "")))
    val ccResult = Confirmer.ccConfirmer.getResult(ccPayload, ccService)

    ccResult.isSuccess should be(true)
    ccResult match {
      case Confirmer.Result.Success(dynamoMap) =>
        dynamoMap("filePaths") should equal(List(s"/some/path/$uuid/file1.txt", s"/some/path/$uuid/file2.txt"))
      case _ => fail("Expected Success result for CC confirmer")
    }
  }

  "CC Confirmer getResult" should "return failure when the payload is valid but the service is invalid" in {
    val uuid = java.util.UUID.randomUUID()
    val ccPayload = CCPayload(uuid)
    val tcService = TCService(null) // Invalid service for CC confirmer
    val ccResult = Confirmer.ccConfirmer.getResult(ccPayload, tcService)

    ccResult.isError should be(true)
    ccResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal(s"Unsupported service in CC confirmer ${uuid.toString}")
      case _ => fail("Expected Failure result for CC confirmer with invalid service")
    }
  }

  "CC Confirmer getResult" should "return failure when the payload is invalid" in {
    val tcPayload = TCPayload(List("file1.txt", "file2.txt")) // Invalid payload for CC confirmer
    val ccService = CCService(TestUtils.ocfl(List(java.util.UUID.randomUUID()), Config("table", RESULT_CC.toString, "", null, "", "")))
    val ccResult = Confirmer.ccConfirmer.getResult(tcPayload, ccService)

    ccResult.isError should be(true)
    ccResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Invalid payload type for CC confirmer")
      case _ => fail("Expected Failure result for CC confirmer with invalid payload")
    }
  }

  "CC Confirmer getResult" should "fail if the paths cannot be found" in {
    val uuid = java.util.UUID.randomUUID()
    val ccPayload = CCPayload(uuid)
    val ccService = CCService(TestUtils.ocfl(List(java.util.UUID.randomUUID()), Config("table", RESULT_CC.toString, "", null, "", ""), OcflErrors(true)))
    val ccResult = Confirmer.ccConfirmer.getResult(ccPayload, ccService)

    ccResult.isError should be(true)
    ccResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal(s"filePaths could not be found for $uuid")
      case _ => fail("Expected Failure result for CC confirmer with invalid payload")
    }
  }

  "TC Confirmer getResult" should "return success when the payload and service is valid" in {
    val filePaths = List("file1.txt", "file2.txt")
    val tcPayload = TCPayload(filePaths)
    val tcService = TCService(TestUtils.scoutAM(filePaths, Config("table", RESULT_TC.toString, "", null, "", "")))
    val tcResult = Confirmer.tcConfirmer.getResult(tcPayload, tcService)

    tcResult.isSuccess should be(true)
    tcResult match {
      case Confirmer.Result.Success(dynamoMap) =>
        dynamoMap("file1.txt") should equal(List("Volume1file1.txt", "Volume2file1.txt"))
        dynamoMap("file2.txt") should equal(List("Volume1file2.txt", "Volume2file2.txt"))
      case _ => fail("Expected Success result for TC confirmer")
    }
  }

  "TC Confirmer getResult" should "return failure when the payload is valid but the service is invalid" in {
    val filePaths = List("file1.txt", "file2.txt")
    val tcPayload = TCPayload(filePaths)
    val ccService =
      CCService(TestUtils.ocfl(List(java.util.UUID.randomUUID()), Config("table", RESULT_CC.toString, "", null, "", ""))) // Invalid service for TC confirmer
    val tcResult = Confirmer.tcConfirmer.getResult(tcPayload, ccService)

    tcResult.isError should be(true)
    tcResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Unsupported service in TC confirmer")
      case _ => fail("Expected Failure result for TC confirmer with invalid service")
    }
  }

  "TC Confirmer getResult" should "return failure when the payload is invalid" in {
    val ccPayload = CCPayload(java.util.UUID.randomUUID()) // Invalid payload for TC confirmer
    val tcService = TCService(TestUtils.scoutAM(List("file1.txt", "file2.txt"), Config("table", RESULT_TC.toString, "", null, "", "")))
    val tcResult = Confirmer.tcConfirmer.getResult(ccPayload, tcService)

    tcResult.isError should be(true)
    tcResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Invalid payload type for TC confirmer")
      case _ => fail("Expected Failure result for TC confirmer with invalid payload")
    }
  }

  "TC Confirmer getResult" should "return failure when volumes cannot be found" in {
    val filePaths = List("file1.txt", "file2.txt")
    val tcPayload = TCPayload(filePaths)
    val tcService = TCService(TestUtils.scoutAM(filePaths, Config("table", RESULT_TC.toString, "", null, "", ""), ScoutAmErrors(true)))
    val tcResult = Confirmer.tcConfirmer.getResult(tcPayload, tcService)

    tcResult.isError should be(true)
    tcResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Volumes could not be found for one or more files in [file1.txt, file2.txt]")
      case _ => fail("Expected Failure result for TC confirmer with invalid payload")
    }
  }
}
