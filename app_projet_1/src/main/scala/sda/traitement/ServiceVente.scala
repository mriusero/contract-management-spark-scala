package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC(): DataFrame = {
      dataFrame
        .withColumn("HTT", regexp_replace(col("HTT"), ",", ".").cast("double"))
        .withColumn("TVA", regexp_replace(col("TVA"), ",", ".").cast("double"))
        .withColumn("TTC", round(col("HTT") + col("TVA") * col("HTT"), 2))
        .drop("HTT", "TVA")
    }

    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)
      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      dataFrame
        .withColumn("MetaTransaction", from_json(col("MetaData"), schema))
        .withColumn("MetaTransaction", col("MetaTransaction.MetaTransaction"))
        .withColumn("Ville", expr("filter(MetaTransaction, x -> x.Ville is not null)[0].Ville"))
        .withColumn("Date_End_contrat_to_reshape", expr("filter(MetaTransaction, x -> x.Date_End_contrat is not null)[0].Date_End_contrat"))
        .withColumn("Date_End_contrat", regexp_extract(col("Date_End_contrat_to_reshape"), "(\\d{4}-\\d{2}-\\d{2})", 0))
        .drop("MetaData", "MetaTransaction", "Date_End_contrat_to_reshape")
    }

  }

}