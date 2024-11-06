// Databricks notebook source
//    SCALA PRACTICE (Learning)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import spark.sqlContext.implicits
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._


// COMMAND ----------

// Part 1: Load data
// Read a JSON file and return two DataFrames, one containing transaction information and the other containing quote information

// COMMAND ----------

val pathJSON = "dbfs:/FileStore/shared_uploads/data.json"

def readerjson(path: String):Seq[DataFrame] = {
  val dataset = spark.read.option("multiline", true).json(path)
  val dftransaction = dataset.select(explode(col("Transaction")).as("Transaction")).select($"Transaction.*")
  val dftauxdevis = dataset.select(explode(col("Devis")).as("Devis")).select($"Devis.*")
  Seq(dftransaction, dftauxdevis)
}

val dftransaction = readerjson(pathJSON)(0)
dftransaction.show

val dftauxdevis = readerjson(pathJSON)(1)
dftauxdevis.show


// COMMAND ----------

// Part 2: Simple aggregation
// Calculate the number of orders for each product in each country

// COMMAND ----------

def NbCommandeParProduitDansPay(df: DataFrame): DataFrame = {
  val dfgrouped = df.groupBy("TypeProduit")
                          .pivot("Pays")
                          .agg(count("IdTransaction").as("Number_Commande"))
                          .na.fill(0)
  return dfgrouped
}
NbCommandeParProduitDansPay(dftransaction).show


// COMMAND ----------

// Part 3: Cross data
// Join the DataFrame dftransaction with dftauxdevis to convert the prices to euros

// COMMAND ----------

def PrixEur (dftransaction : DataFrame, dftauxdevis : DataFrame):DataFrame = {
  val dfjoined = dftransaction.join(dftauxdevis, "Devis")
  val dfconverted = dfjoined.withColumn("PrixEUR", col("Prix") * col("Taux"))
  return dfconverted
}
val dfconverted = PrixEur(dftransaction, dftauxdevis)
dfconverted.show() 

// COMMAND ----------

// Part 4: Aggregation with window partition
// Provide the information of the top two transactions that generated the most money for each country

// COMMAND ----------

def Top2TransactionPerPays (df: DataFrame):DataFrame = {
  val windowSpec = Window.partitionBy("Pays").orderBy(desc("PrixEUR"))
  val dfsorted=df.withColumn("rank", row_number.over(windowSpec))
              .filter($"rank"<=2)
  return(dfsorted)
}
Top2TransactionPerPays(dfconverted).show

// COMMAND ----------

// Part 5: Combined aggregation

// COMMAND ----------

// Perform a combined aggregation on TypeProduit and Pays using the cube function

def Cube (df: DataFrame):DataFrame = {
  val dfcube = df.cube("Pays", "TypeProduit").agg(sum("PrixEur") as "Chiffre_Affaire_EUR")
                    .na.fill(Map("TypeProduit"->"AllProduit", "Pays"->"AllPays"))
                    //.orderBy(desc("Chiffre_Affaire_EUR"))
  return dfcube
}
Cube(dfconverted).show


// COMMAND ----------

// Create the graph of the combined aggregation using the SQL API

val cubeDF = Cube(dfconverted)

cubeDF.createOrReplaceTempView("cube_view")
val resultDF = spark.sql("SELECT CONCAT(Pays, ' - ', TypeProduit) AS Category, Chiffre_Affaire_EUR FROM cube_view")

display(resultDF)

