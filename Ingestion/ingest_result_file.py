# Databricks notebook source
# DBTITLE 1,Loading configurations
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading Common Functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# DBTITLE 1,Adding Widget to receive parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# Importing datatypes
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType
)

# COMMAND ----------

# Creating Schema
result_schema = StructType(
    fields=[
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("constructorId", IntegerType(), False),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), False),
        StructField("positionOrder", IntegerType(), False),
        StructField("points", FloatType(), False),
        StructField("laps", IntegerType(), False),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", FloatType(), True),
        StructField("statusId", IntegerType(), True),
    ]
)

# COMMAND ----------

# Reading file from adls
result_df = spark.read.format('json').load(f'{raw_folder_path}/results.json')

# COMMAND ----------

# Importing functions
from pyspark.sql.functions import col

# Selecting, Renaming and deleting the required columns
selected_result_df = result_df.select(
    col("resultId").alias("result_id"),
    col("raceId").alias("race_id"),
    col("driverId").alias("driver_id"),
    col("constructorId").alias("constructor_id"),
    col("number"),
    col("grid"),
    col("position"),
    col("positionText").alias("position_text"),
    col("positionOrder").alias("position_order"),
    col("points"),
    col("laps"),
    col("time"),
    col("milliseconds"),
    col("fastestLap").alias("fastest_lap"),
    col("rank"),
    col("fastestLapTime").alias("fastest_lap_time"),
    col("fastestLapSpeed").alias("fastest_lap_speed")
)

# COMMAND ----------

# Adding ingestion_date and data_source column
final_result_df = add_ingestion_date(selected_result_df)
final_result_df = add_datasource(final_result_df,v_data_source)

# COMMAND ----------

# Save file to adls
final_result_df.write.parquet(f'{processed_folder_path}/results', mode= "overwrite", partitionBy = "race_id")

# COMMAND ----------

# Read file to adls
spark.read.parquet(f'{processed_folder_path}/results').display()    

# COMMAND ----------

dbutils.notebook.exit("Success")
