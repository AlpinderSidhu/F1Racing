# Databricks notebook source
# DBTITLE 1,Loading configurations
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#import required datatypes
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,DayTimeIntervalType

# COMMAND ----------

# create schema
races_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

# Reading Races.csv from ADLS
races_df =  spark.read.format("csv") \
    .option("header", True) \
    .schema(races_schema) \
    .load(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# Creating timestamp column, extracting required columns and renaming them
from pyspark.sql.functions import to_timestamp, concat, col, lit

races_selected_df = races_df.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    to_timestamp(
        concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"
    ).alias("race_timestamp")
)

# COMMAND ----------

races_final_df=add_ingestion_date(races_selected_df)

# COMMAND ----------

#save file to adls
races_final_df.write.mode("overwrite").parquet("{processed_folder_path}/races")
