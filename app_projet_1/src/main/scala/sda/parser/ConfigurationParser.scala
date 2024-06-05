package sda.parser
import sda.reader._

object ConfigurationParser {
  def getCsvReaderConfigurationFromJson(path: String): CsvReader = {
    val config = parseConfig(path)
    CsvReader(
      path = config("path").toString,
      delimiter = config.get("delimiter").map(_.toString),
      header = config.get("header").map(_.toString.toBoolean)
    )
  }
  def getJsonReaderConfigurationFromJson(path: String): JsonReader = {
    val config = parseConfig(path)
    JsonReader(path = config("path").toString)
  }
  def getOrcReaderConfigurationFromJson(path: String): OrcReader = {
    val config = parseConfig(path)
    OrcReader(path = config("path").toString)
  }
  def getParquetReaderConfigurationFromJson(path: String): ParquetReader = {
    val config = parseConfig(path)
    ParquetReader(path = config("path").toString)
  }
  def getXmlReaderConfigurationFromJson(path: String): XmlReader = {
    val config = parseConfig(path)
    XmlReader(
      path = config("path").toString,
      rootElement = config.get("rootElement").map(_.toString),
      rowElement = config.get("rowElement").map(_.toString)
    )
  }
  private def parseConfig(path: String): Map[String, Any] = {
    val source = scala.io.Source.fromFile(path)
    val jsonString = try source.mkString finally source.close()
    import scala.util.parsing.json.JSON
    JSON.parseFull(jsonString) match {
      case Some(data: Map[String, Any]) => data
      case _ => throw new Exception(s"Could not parse configuration file: $path")
    }
  }
}


