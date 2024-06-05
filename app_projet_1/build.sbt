name := "app_projet_1"
version := "1.0"
scalaVersion := "2.12.15"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "com.beust" % "jcommander" % "1.48",
  "com.databricks" %% "spark-xml" % "0.14.0"
)
