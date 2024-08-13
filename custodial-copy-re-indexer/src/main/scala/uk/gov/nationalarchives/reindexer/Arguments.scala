package uk.gov.nationalarchives.reindexer

import cats.data.Validated
import cats.implicits.*
import com.monovore.decline.*
import uk.gov.nationalarchives.reindexer.Configuration.EntityType

import javax.xml.xpath.{XPathExpression, XPathFactory}
import scala.util.{Failure, Success, Try}

object Arguments:
  private def validatedArgument[T](argName: String)(fn: String => T): Argument[T] =
    Argument.from[T](argName) { arg =>
      Try(fn(arg)) match
        case Failure(_)     => Validated.invalidNel[String, T](s"$arg is not a valid value for $argName")
        case Success(value) => Validated.validNel[String, T](value)
    }

  given Argument[EntityType] = validatedArgument("entity-type")(EntityType.valueOf)

  given Argument[XPathExpression] = validatedArgument("xpath")(XPathFactory.newInstance.newXPath.compile)

  case class ReIndexArgs(fileType: EntityType, columnName: String, xpath: XPathExpression)

  private val fileType: Opts[EntityType] = Opts.option[EntityType]("entity-type", "Location of the field to be indexed. Possible values are IO or CO")
  private val columnName: Opts[String] =
    Opts.option[String]("column-name", "The name of the database column to write the value to").mapValidated { columnName =>
      if columnName.isEmpty then Validated.invalidNel("The column name cannot be blank") else Validated.validNel(columnName)
    }
  private val xPath: Opts[XPathExpression] = Opts.option[XPathExpression]("xpath", "The xpath to the value used to populate the database column")

  val index: Opts[ReIndexArgs] = Opts.subcommand("reindex", "Adds a new value from the metadata xml into the specified database column") {
    (fileType, columnName, xPath).mapN(ReIndexArgs.apply)
  }
