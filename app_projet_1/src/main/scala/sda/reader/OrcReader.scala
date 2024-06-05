package sda.reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class OrcReader(path: String) extends Reader {
  val format = "orc"
  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read.format(format).load(path)
  }
}
