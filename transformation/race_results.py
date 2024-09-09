# Databricks notebook source
# DBTITLE 1,Loading configurations
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# Reading data from processed layer
circuits_df = spark.read.parquet("/mnt/formula1dl7/processed/circuits")
constructors_df = spark.read.parquet("/mnt/formula1dl7/processed/constructors")
drivers_df = spark.read.parquet("/mnt/formula1dl7/processed/drivers")
races_df = spark.read.parquet("/mnt/formula1dl7/processed/races")
results_df = spark.read.parquet("/mnt/formula1dl7/processed/results")

# COMMAND ----------

from pyspark.sql.functions import date_format, current_timestamp

final_df = (
    races_df.join(circuits_df, [races_df.circuit_id == circuits_df.circuit_id], "inner")
    .join(results_df, [races_df.race_id == results_df.race_id], "inner")
    .join(drivers_df, [results_df.driver_id == drivers_df.driver_id], "inner")
    .join(
        constructors_df,
        [results_df.constructor_id == constructors_df.constructor_id],
        "inner",
    )
    .select(
        races_df["race_year"],
        races_df["name"].alias("race_name"),
        date_format(races_df["race_timestamp"], "MM/dd/yyy").alias("race_date"),
        circuits_df["location"].alias("circuit_location"),
        drivers_df["name"].alias("driver_name"),
        drivers_df["number"].alias("driver_number"),
        drivers_df["nationality"].alias("driver_nationality"),
        constructors_df["name"].alias("constructor_name"),
        results_df["grid"],
        results_df["fastest_lap"],
        results_df["time"].alias("race_time"),
        results_df["points"],
        current_timestamp().alias("created_date"),
    )
)


# COMMAND ----------

# Writing data to the presentation layer
# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results/")

# Writing to managed table
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
