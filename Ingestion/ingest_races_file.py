# Databricks notebook source
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
    .load("/mnt/formula1dl7/raw/races.csv")

# COMMAND ----------

# Selecting requried columns, changing column names, creating a timestamp column and adding ingestion_date
from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

races_final_df = races_df.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    to_timestamp(
        concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"
    ).alias("race_timestamp"),
    current_timestamp().alias('ingestion_date')
)

# COMMAND ----------

#save file to adls
races_final_df.write.mode("overwrite").parquet('/mnt/formula1dl7/processed/races')
