package sda.main
import org.apache.spark.sql.SparkSession
import sda.args._
import sda.parser.ConfigurationParser
import  sda.traitement.ServiceVente._

object MainBatch {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("SDA")
      .config("spark.master", "local")
      .getOrCreate()
    Args.parseArguments(args)
    val reader = Args.readertype match {
      case "csv" => {ConfigurationParser.getCsvReaderConfigurationFromJson(Args.readerConfigurationFile)}
      case "json" => {ConfigurationParser.getJsonReaderConfigurationFromJson(Args.readerConfigurationFile)}
      case "orc" => {ConfigurationParser.getOrcReaderConfigurationFromJson(Args.readerConfigurationFile)}
      case "parquet" => {ConfigurationParser.getParquetReaderConfigurationFromJson(Args.readerConfigurationFile)}
      case "xml" => {ConfigurationParser.getXmlReaderConfigurationFromJson(Args.readerConfigurationFile)}
      case _ => throw new Exception("Invalid reader type. Supported reader formats: csv, json, orc, xml, and parquet")
    }
    val df=reader.read().formatter()
    println("***********************Resultat Question1*****************************")
    df.show(20)
    println("***********************Resultat Question2*****************************")
    df.calculTTC().show(20)
    println("***********************Resultat Question3*****************************")
    df.calculTTC.extractDateEndContratVille.show
    println("***********************Resultat Question4*****************************")
    df.calculTTC.extractDateEndContratVille.contratStatus.show(20)
    }
}
