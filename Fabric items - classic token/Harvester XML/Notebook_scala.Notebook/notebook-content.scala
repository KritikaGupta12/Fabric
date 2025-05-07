// Fabric notebook source

// METADATA ********************

// META {
// META   "kernel_info": {
// META     "name": "synapse_pyspark"
// META   },
// META   "dependencies": {
// META     "lakehouse": {
// META       "default_lakehouse": "49633d52-5992-4ab2-abdb-6fa12095c04c",
// META       "default_lakehouse_name": "LH_Harvester",
// META       "default_lakehouse_workspace_id": "a2fdbf7b-89ff-4006-9e9d-102ccb49ae4a"
// META     },
// META     "environment": {
// META       "environmentId": "d8d16304-82a4-a21a-47de-9d0e018d0943",
// META       "workspaceId": "00000000-0000-0000-0000-000000000000"
// META     }
// META   }
// META }

// MARKDOWN ********************

// # Version check

// CELL ********************

util.Properties.versionString
util.Properties.versionNumberString
util.Properties.versionMsg

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

// %%spark
// spark-submit \
// --packages com.databricks:spark-xml_2.12-0.14.0 \ 


// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// MARKDOWN ********************

// # Scala config

// CELL ********************


// %%configure -f
// {
//     "conf": 
//     {
//         "spark.jars.packages": "com.databricks:spark-xml_2.12-0.15.0"
//     }
// }

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

val spark = SparkSession.builder()
  .appName("harvester")
  .config("spark.jars.packages", "com.databricks:spark-xml_2.12-0.15.0")
  .getOrCreate()


// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val df_scala = spark.read.format("com.databricks.spark.xml").option("rowTag", "book").load("Files/books.xml")
display(df_scala)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val file1 = "Files/20211001_gpx103-sdcgpx1852-47b994e1_2296_4d13_8b9e_e5ef5ed3449e-X1.12-9510015292_20211001_Rödvattenmyran.xml"

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val df_scala = spark.read.format("com.databricks.spark.xml").option("rowTag", "Machine").load(file1)
display(df_scala)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val df_scala = spark.read.format("com.databricks.spark.xml").option("rowTag", "Stem").load(file1)
display(df_scala)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.format('xml').options(rowTag='book').load('books.xml')
df.select("author", "_id").write \
    .format('xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }
