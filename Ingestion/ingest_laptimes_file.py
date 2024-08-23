# Databricks notebook source
# DBTITLE 1,Loading configurations
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Importing Datatypes
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp
# Defining Schema
lap_times_schema = StructType(fields = [
    StructField('race_id',IntegerType(),False),
    StructField('driver_id',IntegerType(),True),
    StructField('lap',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True)
])

# COMMAND ----------

# Reading from adls
lap_times_df = spark.read \
    .format('csv') \
    .schema(lap_times_schema) \
    .load(f'{raw_folder_path}/lap_times/*.csv')

# COMMAND ----------

# Adding ingestion_date column
lap_times_final_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

# Writing to ADLS
lap_times_final_df.write.mode('overwrite').parquet(processed_folder_path+"/lap_times/")