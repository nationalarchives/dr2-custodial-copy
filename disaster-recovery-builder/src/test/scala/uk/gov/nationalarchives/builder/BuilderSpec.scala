package uk.gov.nationalarchives.builder

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.builder.Main.{Config, Message}
import uk.gov.nationalarchives.utils.Utils.OcflFile
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import java.nio.file.Files
import java.time.Instant
import java.util.UUID

class BuilderSpec extends AnyFlatSpec:

  "Builder run" should "call the Ocfl and Database methods with the correct arguments" in {
    val config = Config("", "", Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString)
    val idOne = UUID.randomUUID
    val idTwo = UUID.randomUUID
    val ids = List(idOne, idTwo)
    val fileId = UUID.randomUUID()

    given Ocfl[IO] = new Ocfl[IO](config):
      override def generate(id: UUID): IO[List[OcflFile]] = IO {
        ids.contains(id) should equal(true)
        List(OcflFile(1, id, "name".some, fileId, "zref".some, "path".some, "fileName".some, Instant.now.some, "sourceId".some, "citation".some))
      }
    given Database[IO] = (files: List[OcflFile]) =>
      IO {
        files.length == 2 should equal(true)
        assert(files.map(_.id).sorted == ids.sorted)
      }

    val messages = List(MessageResponse[Message]("", Message(idOne)), MessageResponse[Message]("", Message(idTwo)))
    Builder[IO].run(messages).unsafeRunSync()
  }

  "Builder run" should "not call the Ocfl generate method and will call the the database method with an empty list if an empty list is passed" in {
    val config = Config("", "", Files.createTempDirectory("work").toString, Files.createTempDirectory("repo").toString)
    val idOne = UUID.randomUUID
    val idTwo = UUID.randomUUID
    val ids = List(idOne, idTwo)

    given Ocfl[IO] = new Ocfl[IO](config):
      override def generate(id: UUID): IO[List[OcflFile]] = IO {
        assert(false)
        Nil
      }

    given Database[IO] = (files: List[OcflFile]) =>
      IO {
        files.isEmpty should be(true)
      }

    Builder[IO].run(Nil).unsafeRunSync()
  }
