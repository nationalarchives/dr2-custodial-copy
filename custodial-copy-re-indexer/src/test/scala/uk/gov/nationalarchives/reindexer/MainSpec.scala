package uk.gov.nationalarchives.reindexer

import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, IO}
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.{Command, Opts}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.gov.nationalarchives.reindexer.Arguments.index

class MainSpec extends AnyFlatSpec with EitherValues with TableDrivenPropertyChecks:
  object MainTest extends CommandIOApp(name = "test-app", header = ""):
    override def main: Opts[IO[ExitCode]] = index.map(_ => IO(ExitCode.Success))

  def runInvalidMainTest(args: String*): List[String] =
    Command("", "")(MainTest.main)
      .parse(args)
      .left
      .value
      .errors

  val entityTypeTable: TableFor1[String] = Table(
    "fileType",
    "IO",
    "CO"
  )

  forAll(entityTypeTable) { entityType =>
    "Main" should s"return success if the arguments are valid for entity-type $entityType" in {
      Command("", "")(MainTest.main)
        .parse(List("reindex", "--entity-type", entityType, "--column-name", "test", "--xpath", "//Valid"))
        .value
        .unsafeRunSync()
        .code should equal(0)
    }
  }

  "Main" should "return an error if the entity-type is not IO or CO" in {
    val errors = runInvalidMainTest("reindex", "--entity-type", "SO", "--column-name", "test", "--xpath", "//Valid")
    errors should equal(List("SO is not a valid value for entity-type"))
  }

  "Main" should "return an error if the xpath is invalid" in {
    val errors = runInvalidMainTest("reindex", "--entity-type", "IO", "--column-name", "test", "--xpath", "(")
    errors should equal(List("( is not a valid value for xpath"))
  }

  "Main" should "return an error if the column name is empty" in {
    val errors = runInvalidMainTest("reindex", "--entity-type", "IO", "--column-name", "", "--xpath", "//Valid")
    errors should equal(List("The column name cannot be blank"))
  }

  "Main" should "return an error if the subcommand is missing" in {
    val errors = runInvalidMainTest("--entity-type", "IO", "--column-name", "test", "--xpath", "//Valid")
    errors should equal(List("Unexpected option: --entity-type"))
  }

  "Main" should "return an error if the entity-type is missing" in {
    val errors = runInvalidMainTest("reindex", "--column-name", "test", "--xpath", "//Valid")
    errors should equal(List("Missing expected flag --entity-type!"))
  }

  "Main" should "return an error if the column-name is missing" in {
    val errors = runInvalidMainTest("reindex", "--entity-type", "IO", "--xpath", "//Valid")
    errors should equal(List("Missing expected flag --column-name!"))
  }

  "Main" should "return an error if the xpath is missing" in {
    val errors = runInvalidMainTest("reindex", "--entity-type", "IO", "--column-name", "test")
    errors should equal(List("Missing expected flag --xpath!"))
  }
