package uk.gov.nationalarchives.confirmer

import org.scalatest.matchers.should.Matchers.*
import io.circe.parser.decode

import scala.language.postfixOps

class ModelsTest extends org.scalatest.flatspec.AnyFlatSpec {

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

  "FileResponse decoder" should "decode a FileResponse" in {
    val json =
      """
        |{
        |  "archdone": true,
        |  "copies": [
        |    {"copy": "1"},
        |    {"copy": "2"},
        |    {"copy": "3", "sections": [{"volume": "L03721"}]}
        |  ],
        |  "checksum": "someChecksumValue"
        |}""".stripMargin

    val decoded = decode[FileResponse](json)
    decoded match {
      case Right(fileResponse) =>
        fileResponse.archdone shouldEqual true
        fileResponse.copies should have size 3
        fileResponse.copies.head.copy shouldEqual "1"
        fileResponse.copies.find(_.copy == "3").flatMap(_.sections) shouldEqual Some(List(Section("L03721")))
        fileResponse.checksum shouldEqual Some("someChecksumValue")
      case Left(error) => fail(s"Decoding failed: $error")
    }
  }

  "FileResponse decoder" should "decode a FileResponse without sections" in {
    val json =
      """
        |{
        |  "archdone": false,
        |  "copies": [
        |    {"copy": "1"},
        |    {"copy": "2"}
        |  ],
        |  "checksum": null
        |}""".stripMargin

    val decoded = decode[FileResponse](json)
    decoded match {
      case Right(fileResponse) =>
        fileResponse.archdone shouldEqual false
        fileResponse.copies should have size 2
        fileResponse.copies.head.copy shouldEqual "1"
        fileResponse.copies.find(_.copy == "3") shouldEqual None
        fileResponse.checksum shouldEqual None
      case Left(error) => fail(s"Decoding failed: $error")
    }
  }

  "AuthorisationResponse decoder" should "decode an AuthorisationResponse" in {
    val json =
      """
        |{
        |  "response": "some-authorisation-token"
        |}
        |""".stripMargin

    val decoded = decode[AuthorisationResponse](json)
    decoded match {
      case Right(authResponse) =>
        authResponse.token shouldEqual "some-authorisation-token"
      case Left(error) => fail(s"Decoding failed: $error")
    }
  }

  "Message decoder" should "decode the CC message" in {
    val json =
      """
        |{
        |   "assetId": "141bac2e-bab3-4cec-88fd-2649bda971ea",
        |   "batchId": "some-batch",
        |   "payload": "{\"preservationSystemId\": \"d48de631-6fb2-480b-989b-c3b8f48659ec\"}"
        |}
        |""".stripMargin

    val decoded = decode[OutputQueueMessage](json)
    decoded match {
      case Right(message) =>
        message.payload match {
          case p: CCPayload => p.preservationSystemId.toString shouldEqual "d48de631-6fb2-480b-989b-c3b8f48659ec"
          case other        => fail(s"Expected CCPayload, got ${other.getClass.getSimpleName}")
        }
      case Left(err) => fail(s"Decoding failed: $err")
    }
  }

  "Message decoder" should "decode the TC message" in {
    val json =
      """
        |{
        |   "assetId": "141bac2e-bab3-4cec-88fd-2649bda971ea",
        |   "batchId": "some-batch",
        |   "payload": "{\"filePaths\": [\"/tmp1/file1\", \"/tmp1/file2\"]}"
        |}
        |""".stripMargin
    val decoded = decode[OutputQueueMessage](json)
    decoded match {
      case Right(message) =>
        message.payload match {
          case p: TCPayload => p.filePaths should contain allOf ("/tmp1/file1", "/tmp1/file2")
          case other        => fail(s"Expected TCPayload, got ${other.getClass.getSimpleName}")
        }
      case Left(err) => fail(s"Decoding failed: $err")
    }
  }
}
