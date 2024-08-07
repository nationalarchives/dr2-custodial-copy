package uk.gov.nationalarchives.reindexer

import cats.data.{Validated, ValidatedNel}
import cats.effect.*
import cats.implicits.*
import com.monovore.decline.*
import com.monovore.decline.effect.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*

import javax.xml.xpath.{XPathExpression, XPathFactory}
import scala.util.{Failure, Success, Try}
object Main extends CommandIOApp(name = "cc-indexer", header = "") {

  enum FileType:
    case IO, CO

  private def validatedArgument[T](argName: String)(fn: String => T): Argument[T] =
    Argument.from[T](argName) { string =>
      Try(fn(string)) match
        case Failure(_) => Validated.invalidNel[String, T](s"$string is not a valid value for $argName")
        case Success(value) => Validated.validNel[String, T](value)
    }

  given Argument[FileType] = validatedArgument("file-type")(FileType.valueOf)

  given Argument[XPathExpression] = validatedArgument("xpath")(XPathFactory.newInstance.newXPath.compile)

  case class ReindexArgs(fileType: FileType, columnName: String, xpath: XPathExpression)

  case class Config(databasePath: String, ocflRepoDir: String, ocflWorkDir: String) derives ConfigReader

  private val fileType: Opts[FileType] = Opts.option[FileType]("file-type", "Location of the field to be indexed. Possible values are IO or CO")
  private val columnName: Opts[String] = Opts.option[String]("column-name", "The name of the database column to write the value to")
  private val xPath: Opts[XPathExpression] = Opts.option[XPathExpression]("xpath", "The xpath to the value used to populate the database column")

  val index: Opts[ReindexArgs] = Opts.subcommand("reindex", "Adds a new value from the metadata xml into the specified database column") {
    (fileType, columnName, xPath).mapN(ReindexArgs.apply)
  }

  override def main: Opts[IO[ExitCode]] = index.map {
    case ReindexArgs(fileType, columnName, xpath) => IO.println(s"$fileType $columnName $xpath") >> IO(ExitCode.Success)
  }
}
