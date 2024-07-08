package uk.gov.nationalarchives.builder.utils

import uk.gov.nationalarchives.builder.Main.Config
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId, VersionInfo}
import io.ocfl.api.{OcflConfig, OcflObjectUpdater, OcflOption, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.{OcflStorage, OcflStorageBuilder}

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.jdk.FunctionConverters.*
import scala.xml.Elem

object TestUtils:

  extension (uuid: UUID) def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)

  def repository(repoDir: Path, workDir: Path): OcflRepository = {
    val storage: OcflStorage = OcflStorageBuilder.builder().fileSystem(repoDir).build
    val ocflConfig: OcflConfig = new OcflConfig()
    ocflConfig.setDefaultDigestAlgorithm(DigestAlgorithm.sha256)
    new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(storage)
      .ocflConfig(ocflConfig)
      .prettyPrintJson()
      .workDir(workDir)
      .build()
  }

  val coRef: UUID = UUID.randomUUID
  val coRefTwo: UUID = UUID.randomUUID

  val completeIoMetadataContent: Elem = <Metadata>
    <Identifiers>
      <Identifier>
        <Type>BornDigitalRef</Type>
        <Value>Zref</Value>
      </Identifier>
    </Identifiers>
    <InformationObject>
      <Title>Title</Title>
    </InformationObject>
  </Metadata>
  val completeCoMetadataContentElements: List[Elem] = List(
    <Metadata>
      <ContentObject>
        <Title>Content Title</Title>
        <Ref>{coRef}</Ref>
      </ContentObject>
    </Metadata>,
    <Metadata>
      <ContentObject>
        <Title>Content Title2</Title>
        <Ref>{coRefTwo}</Ref>
      </ContentObject>
    </Metadata>
  )

  def initialiseRepo(
      id: UUID,
      ioMetadataContent: Elem = completeIoMetadataContent,
      coMetadataContent: List[Elem] = completeCoMetadataContentElements
  ): Config = {
    val repoDir = Files.createTempDirectory("repo")
    val workDir = Files.createTempDirectory("work")
    val metadataFileDirectory = Files.createTempDirectory("metadata")
    val ioMetadataFile = Files.createFile(metadataFileDirectory.resolve("IO_Metadata.xml"))

    val contentFile = Files.createTempFile("content", "file")
    Files.write(contentFile, "test".getBytes)

    Files.write(ioMetadataFile, ioMetadataContent.toString.getBytes)

    repository(repoDir, workDir).updateObject(
      id.toHeadVersion,
      new VersionInfo(),
      { (updater: OcflObjectUpdater) =>
        updater.addPath(ioMetadataFile, "IO_Metadata.xml", OcflOption.OVERWRITE)
        coMetadataContent.zipWithIndex.map { (elem, idx) =>
          val coMetadataFile = Files.createFile(metadataFileDirectory.resolve(s"CO_Metadata$idx.xml"))
          Files.write(coMetadataFile, elem.toString.getBytes)
          updater.addPath(coMetadataFile, s"subfolder$idx/CO_Metadata.xml", OcflOption.OVERWRITE)
          updater.addPath(contentFile, s"subfolder$idx/original/g1/content.file", OcflOption.OVERWRITE)
        }
        ()
      }.asJava
    )
    Config("test-database", "http://localhost:9001", repoDir.toString, workDir.toString)
  }
