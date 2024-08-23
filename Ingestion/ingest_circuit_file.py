# Databricks notebook source
# DBTITLE 1,Loading Configurations
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Importing datatypes
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType

# Defining Circuits Schema
circuits_schema = StructType(
    fields=[
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", DoubleType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

# Reading the circuits CSV File from mounted location using the specified schema
circuits_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(circuits_schema) \
    .load(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# Selecting the required columns
circuits_selected_df = circuits_df.drop("url")

# COMMAND ----------

# Renaming columns
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") 


# COMMAND ----------

# Adding a ingestion date column
# from pyspark.sql.functions import current_timestamp
# circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# Writing data to ADLS as parquet
circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder_path}/circuits'))
