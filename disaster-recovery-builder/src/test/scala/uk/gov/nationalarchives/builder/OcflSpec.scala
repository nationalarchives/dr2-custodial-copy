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

    val firstFileOpt = files.find(_.fileId == coRef)
    val secondFileOpt = files.find(_.fileId == coRefTwo)

    firstFileOpt.isDefined should equal(true)
    secondFileOpt.isDefined should equal(true)

    val firstFile = firstFileOpt.get
    val secondFile = secondFileOpt.get

    firstFile.id should equal(id)
    firstFile.name.get should equal("Title")
    firstFile.fileId should equal(coRef)
    firstFile.zref.get should equal("Zref")
    firstFile.fileName.get should equal("Content Title")

    secondFile.id should equal(id)
    secondFile.name.get should equal("Title")
    secondFile.fileId should equal(coRefTwo)
    secondFile.zref.get should equal("Zref")
    secondFile.fileName.get should equal("Content Title2")
  }
