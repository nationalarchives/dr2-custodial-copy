package uk.gov.nationalarchives.confirmer

import org.scalatest.matchers.should.Matchers.*
import io.circe.parser.decode

class ModelsTest extends org.scalatest.flatspec.AnyFlatSpec {
  "ConfirmationOperator" should "return the correct operator type based on the config" in {
    val id = java.util.UUID.randomUUID()
    val operator = ConfirmationOperator.getOperator(
      Config("table", "CC_result", "", null, "", ""),
      TestUtils.ocfl(List(id), Config("table", "CC_result", "", null, "", "")),
      None
    )
    operator shouldBe a[CCOperator]
    val paths: List[String] = operator match {
      case cc: CCOperator => cc.ocfl.getFilePathsForObject(id)
      case _              => fail("Unexpected operator created")
    }
    paths should contain allOf (s"/some/path/$id/file1.txt", s"/some/path/$id/file2.txt")
  }

  "Payload decoder" should "decode a CCPayload" in {
    val id = java.util.UUID.randomUUID()

    val json =
      s"""
         |{
         |  "preservationSystemId": "$id"
         |}
         |""".stripMargin

    val decoded = decode[Payload](json)
    decoded match {
      case Right(payload) =>
        payload shouldBe a[CCPayload]
        payload.asInstanceOf[CCPayload].preservationSystemId shouldEqual id
      case Left(error) => fail(s"Decoding failed: $error")
    }
  }

  "Payload decoder" should "decode a TCPayload" in {
    val json =
      """
        |{
        |  "filePaths": [
        |    "/tmp/file1",
        |    "/tmp/file2"
        |  ]
        |}
        |""".stripMargin

    val decoded = decode[Payload](json)
    decoded match {
      case Right(payload) =>
        payload shouldBe a[TCPayload]
        payload.asInstanceOf[TCPayload].filePaths shouldEqual List("/tmp/file1", "/tmp/file2")
      case Left(error) => fail(s"Decoding failed: $error")
    }
  }

  "Payload decoder" should "fail to decode an unknown payload type" in {
    val json =
      """
        |{
        |  "unknownField": "someValue"
        |}
        |""".stripMargin

    val decoded = decode[Payload](json)
    decoded match {
      case Right(_)    => fail("Decoding should have failed for unknown payload type")
      case Left(error) =>
        error.getMessage should include("Could not determine payload type. Expected either 'preservationSystemId' or 'filePaths'.")
    }
  }
}
