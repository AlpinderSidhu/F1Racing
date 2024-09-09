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

# Import Datatypes
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Defining Name Schema
name_schema = StructType(
    fields=[
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

# Defining Drivers Schema
drivers_schema = StructType(
    fields=[
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema, True),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

# Reading drivers.json file
drivers_df = spark.read.format("json").schema(drivers_schema).load(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

# Renaming Columns, Removing URL column and Transforming name column
from pyspark.sql.functions import col, concat, lit

drivers_selected_df = drivers_df.select(
    col("driverId").alias("driver_id"),
    col("driverRef").alias("driver_ref"),
    col("number"),
    col("code"),
    concat(col("name.forename"), lit(" "), col("name.surname")).alias("name"),
    col("dob"),
    col("nationality")
)

# COMMAND ----------

# Adding ingestion_date and data_source column
drivers_final_df = add_ingestion_date(drivers_selected_df)
drivers_final_df = add_datasource(drivers_final_df,v_data_source)

# COMMAND ----------

# Write the output to the Adls
# drivers_final_df.write.parquet(f"{processed_folder_path}/drivers/", "overwrite")

# Writing to a managed Table
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
