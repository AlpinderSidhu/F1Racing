# Databricks notebook source
# DBTITLE 1,Loading configurations
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# DBTITLE 1,Adding Widget to receive parameters
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

#Importing datatypes
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

#Defining Schema
qualifying_schema = StructType(fields = [
    StructField('qualifyId',IntegerType(),False),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('q1',StringType(),True),
    StructField('q2',StringType(),True),
    StructField('q3',StringType(),True)
])

# COMMAND ----------

# Reading File from ADLS
qualifying_df = spark.read \
    .format('json') \
    .option('multiLine',True) \
    .schema(qualifying_schema) \
    .load(f'{raw_folder_path}/qualifying/*.json')

# COMMAND ----------

# Renaming columns
selected_qualifying_df = qualifying_df \
    .withColumnRenamed( "qualifyId","qualify_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id") 

# COMMAND ----------

# Adding ingestion_date and data_source column
final_qualifying_df = add_ingestion_date(selected_qualifying_df)
final_qualifying_df = add_datasource(final_qualifying_df,v_data_source)

# COMMAND ----------

# Writing to ADLS
final_qualifying_df.write.mode("overwrite").parquet(f'{processed_folder_path}/qualifying/')

# COMMAND ----------

# Reading data from ADLS
# spark.read.parquet(f'{processed_folder_path}/qualifying/').display()

# COMMAND ----------

dbutils.notebook.exit("Success")
