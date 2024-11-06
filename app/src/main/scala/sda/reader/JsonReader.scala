package sda.reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class JsonReader(path: String,
                     delimiter: Option[String] = None,
                     header:Option[Boolean] = None
                    )
  extends Reader {
  val format = "json"
  def read()(implicit spark: SparkSession): DataFrame = {
    val df = spark.read.format(format)
      .option("multiline", true)
      .load(path)
    val desiredColumnOrder = Seq("Id_Client", "HTT_TVA", "MetaData")
    df.select(desiredColumnOrder.map(df.col): _*)
  }
}
