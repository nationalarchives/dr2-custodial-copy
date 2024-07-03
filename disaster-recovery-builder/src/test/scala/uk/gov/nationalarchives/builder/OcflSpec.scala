package uk.gov.nationalarchives.builder

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.builder.Main.Config
import uk.gov.nationalarchives.builder.utils.TestUtils.*
import cats.effect.unsafe.implicits.global
import org.scalatest.matchers.should.Matchers.*
import java.util.UUID

class OcflSpec extends AnyFlatSpec:

  "generate" should "return the correct values" in {
    val id = UUID.randomUUID
    val testConfig = initialiseRepo(id)
    given Configuration = new Configuration:
      override def config: Config = testConfig

    val files = Ocfl[IO].generate(id).unsafeRunSync()
    val firstFile = files.minBy(_.fileId)
    val secondFile = files.maxBy(_.fileId)

    firstFile.id should equal(id.toString)
    firstFile.name should equal("Title")
    firstFile.fileId should equal("Reference")
    firstFile.zref should equal("Zref")
    firstFile.fileName should equal("Content Title")

    secondFile.id should equal(id.toString)
    secondFile.name should equal("Title")
    secondFile.fileId should equal("Reference2")
    secondFile.zref should equal("Zref")
    secondFile.fileName should equal("Content Title2")
  }
