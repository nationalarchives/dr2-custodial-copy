package uk.gov.nationalarchives.confirmer

import uk.gov.nationalarchives.confirmer.ResultAttributeName.TC_RESULT
import org.scalatest.matchers.should.Matchers.*
import io.circe.parser.decode

class ModelsTest extends org.scalatest.flatspec.AnyFlatSpec {
  "ConfirmationService" should "return the CC service type based on the config" in {
    val id = java.util.UUID.randomUUID()
    val service = ConfirmationService.getInstance(
      Config("table", "CC_result", "", null, "", ""),
      TestUtils.ocfl(List(id), Config("table", "CC_result", "", null, "", "")),
      null
    )

    service shouldBe a[CCService]
    val paths: List[String] = service match {
      case cc: CCService => cc.ocfl.getFilePathsForObject(id)
      case _             => fail("Unexpected service created")
    }
    paths should contain allOf (s"/some/path/$id/file1.txt", s"/some/path/$id/file2.txt")
  }

  "ConfirmationService" should "return the TC service type based on the config" in {
    val service = ConfirmationService.getInstance(
      Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password")),
      null,
      TestUtils.scoutAM(
        List("/tmp/file1", "/tmp/file2"),
        Config("table", TC_RESULT.toString, "", null, "", "", Some("http://scout.base.url:8080"), Some("scout.username"), Some("scout.password"))
      )
    )
    service shouldBe a[TCService]

    val result = service match {
      case tc: TCService => tc.scoutAM.getFileDetails(List("/tmp/file1", "/tmp/file2"))
      case _             => fail("Unexpected service created")
    }
    result should contain allOf ("/tmp/file1" -> List("Volume1/tmp/file1", "Volume2/tmp/file1"), "/tmp/file2" -> List("Volume1/tmp/file2", "Volume2/tmp/file2"))
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
}
