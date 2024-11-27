package uk.gov.nationalarchives.reindexer

import cats.effect.*
import cats.implicits.*
import com.monovore.decline.*
import org.w3c.dom.Document
import pureconfig.*
import uk.gov.nationalarchives.reindexer.Configuration.Config

import java.util.UUID
import javax.xml.xpath.{XPathExpression, XPathFactory}

trait Configuration:
  private[reindexer] lazy val configOrError: Config =
    ConfigSource.default.loadOrThrow[Config]

  def config: Config
object Configuration:

  def apply[F[_]: Async](using ev: Configuration): Configuration = ev

  given impl: Configuration = new Configuration:
    override def config: Config = configOrError

  case class Config(databasePath: String, ocflRepoDir: String, ocflWorkDir: String) derives ConfigReader

  trait ReIndexUpdate:
    val id: UUID
    val value: String

  case class CoUpdate(id: UUID, value: String) extends ReIndexUpdate

  case class IoUpdate(id: UUID, value: String) extends ReIndexUpdate

  enum EntityType:
    def getReindexUpdate(xpathExpression: XPathExpression, document: Document): ReIndexUpdate = {
      val value = xpathExpression.evaluate(document)

      def ref(refXpathPrefix: String): UUID = UUID.fromString(
        XPathFactory.newInstance.newXPath
          .compile(s"//$refXpathPrefix/Ref")
          .evaluate(document)
      )

      this match
        case EntityType.IO => IoUpdate(ref("InformationObject"), value)
        case EntityType.CO => CoUpdate(ref("ContentObject"), value)
    }

    case IO, CO
