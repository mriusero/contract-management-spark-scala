package sda.reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ParquetReader(path: String) extends Reader {
  val format = "parquet"
  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read.format(format).load(path)
  }
}

