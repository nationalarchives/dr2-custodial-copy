package uk.gov.nationalarchives.confirmer

import uk.gov.nationalarchives.confirmer.ResultAttributeName.{result_CC, result_TC}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.confirmer.TestUtils.{OcflErrors, ScoutAmErrors}

class ConfirmerTest extends AnyFlatSpec {
  "getConfirmer" should "return correct instance of confirmer based on the config type" in {
    Confirmer.getConfirmer(CCConfig("table", result_CC.toString, "", null, "", "")).getClass.getDeclaredFields.apply(1).getType.getSimpleName should equal(
      "Ocfl"
    )
    Confirmer.getConfirmer(TCConfig("table", result_TC.toString, "", null, "", "", "")).getClass.getDeclaredFields.apply(1).getType.getSimpleName should equal(
      "ScoutAM"
    )
  }

  "CC Confirmer getResult" should "return success when the payload and service is valid" in {
    val uuid = java.util.UUID.randomUUID()
    val ccPayload = CCPayload(uuid)
    val ocfl = TestUtils.ocfl(List(uuid), CCConfig("table", result_CC.toString, "", null, "", ""))
    val ccResult = Confirmer.ccConfirmer(ocfl).getResult(ccPayload)

    ccResult.isSuccess should be(true)
    ccResult match {
      case Confirmer.Result.Success(dynamoMap) =>
        dynamoMap("filePaths") should equal(List(s"/some/path/$uuid/file1.txt", s"/some/path/$uuid/file2.txt"))
      case _ => fail("Expected Success result for CC confirmer")
    }
  }

  "CC Confirmer getResult" should "return failure when the payload is invalid" in {
    val tcPayload = TCPayload(List("file1.txt", "file2.txt")) // Invalid payload for CC confirmer
    val ocfl = TestUtils.ocfl(List(java.util.UUID.randomUUID()), CCConfig("table", result_CC.toString, "", null, "", ""))
    val ccResult = Confirmer.ccConfirmer(ocfl).getResult(tcPayload)

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
    val ocfl = TestUtils.ocfl(List(java.util.UUID.randomUUID()), CCConfig("table", result_CC.toString, "", null, "", ""), OcflErrors(true))
    val ccResult = Confirmer.ccConfirmer(ocfl).getResult(ccPayload)

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
    val scoutAM = TestUtils.scoutAM(filePaths, TCConfig("table", result_TC.toString, "", null, "", "", ""))
    val tcResult = Confirmer.tcConfirmer(scoutAM).getResult(tcPayload)

    tcResult.isSuccess should be(true)
    tcResult match {
      case Confirmer.Result.Success(dynamoMap) =>
        dynamoMap("file1.txt") should equal(List("Volume1file1.txt", "Volume2file1.txt"))
        dynamoMap("file2.txt") should equal(List("Volume1file2.txt", "Volume2file2.txt"))
      case _ => fail("Expected Success result for TC confirmer")
    }
  }

  "TC Confirmer getResult" should "return failure when the payload is invalid" in {
    val ccPayload = CCPayload(java.util.UUID.randomUUID()) // Invalid payload for TC confirmer
    val scountAM = TestUtils.scoutAM(List("file1.txt", "file2.txt"), TCConfig("table", result_TC.toString, "", null, "", "", ""))
    val tcResult = Confirmer.tcConfirmer(scountAM).getResult(ccPayload)

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
    val scoutAM = TestUtils.scoutAM(filePaths, TCConfig("table", result_TC.toString, "", null, "", "", ""), ScoutAmErrors(true))
    val tcResult = Confirmer.tcConfirmer(scoutAM).getResult(tcPayload)

    tcResult.isError should be(true)
    tcResult match {
      case Confirmer.Result.Failure(errorMessage) =>
        errorMessage.getMessage should equal("Volumes could not be found for one or more files in [file1.txt, file2.txt]")
      case _ => fail("Expected Failure result for TC confirmer with invalid payload")
    }
  }
}
