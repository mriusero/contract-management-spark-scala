package sda.reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class XmlReader(path: String,
                     rootElement: Option[String] = None,
                     rowElement: Option[String] = None
                    ) extends Reader {
  val format = "xml"
  def read()(implicit spark: SparkSession): DataFrame = {
    val df = spark.read.format(format)
      .option("rootTag", rootElement.getOrElse("data"))
      .option("rowTag", rowElement.getOrElse("record"))
      .load(path)
    val desiredColumnOrder = Seq("Id_Client", "HTT_TVA", "MetaData")
    df.select(desiredColumnOrder.map(df.col): _*)
  }
}

