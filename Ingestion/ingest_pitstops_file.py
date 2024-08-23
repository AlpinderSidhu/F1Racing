# Databricks notebook source
# DBTITLE 1,Loading configurations
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Importing Datatypes
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Defining Schema
pit_stops_schema = StructType(fields={
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),False),
    StructField("stop",IntegerType(),False),
    StructField("lap",IntegerType(),False),
    StructField("time",StringType(),False),
    StructField("duration",StringType(),True),
    StructField("milliseconds",IntegerType(),True)
})

# COMMAND ----------

# Reading file from ADLS
pit_stops_df = (
    spark.read.format("json")
    .schema(pit_stops_schema)
    .option("multiLine", True)
    .load(f"{raw_folder_path}/pit_stops.json")
)

# COMMAND ----------

# Selecting and renaming required columns
selected_pit_stops_df = pit_stops_df \
    .withColumnRenamed('driverId','driver_id') \
    .withColumnRenamed('raceId','race_id') 

# COMMAND ----------

# Adding ingestion date column
final_pit_stops_df = add_ingestion_date(selected_pit_stops_df)

# COMMAND ----------

# Writing data in ADLS
final_pit_stops_df.write.mode("overwrite").parquet( processed_folder_path + "/pit_stops")
