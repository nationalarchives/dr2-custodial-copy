package uk.gov.nationalarchives.utils

object Utils:
  final case class OcflFile(versionNum: Long, id: String, name: String, fileId: String, zref: String, path: String, fileName: String)
