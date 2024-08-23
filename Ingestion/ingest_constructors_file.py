# Databricks notebook source
# DBTITLE 1,Loading Configuration
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# import datatypes
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

#define schema
schema = StructType(fields = [
    StructField("constructorId",StringType(),True),
    StructField("constructorRef",StringType(),True),
    StructField("name",StringType(),True),
    StructField("nationality",StringType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

# Read File
constructors_df = (
    spark.read.format("json")
    .schema(schema)
    .load(f"{raw_folder_path}/constructors.json")
)

# COMMAND ----------

# Rename columns
from pyspark.sql.functions import col
constructors_renamed_df = constructors_df.select(
    col("constructorId").alias("constructor_id"),
    col("constructorRef").alias("constructor_ref"),
    col("name"),
    col("nationality")
)

# COMMAND ----------

# Adding a ingestion date column
constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# Write File
constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors/")