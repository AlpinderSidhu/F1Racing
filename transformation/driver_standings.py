# Databricks notebook source
# DBTITLE 1,Loading configuration
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# DBTITLE 1,Loading common functions
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Reading data from processed layer
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
races_df = spark.read.parquet(f"{processed_folder_path}/races")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

from pyspark.sql.functions import date_format, current_timestamp

race_result_df = (
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
        drivers_df["nationality"].alias("driver_nationality"),
        drivers_df["name"].alias("driver_name"),
        constructors_df["name"].alias("constructor_name"),
        results_df["position"],
        results_df["points"],
        circuits_df["location"].alias("circuit_location"),
        drivers_df["number"].alias("driver_number"),
        results_df["grid"],
        results_df["fastest_lap"],
        results_df["time"].alias("race_time"),
        date_format(races_df["race_timestamp"], "MM/dd/yyy").alias("race_date"),
        current_timestamp().alias("created_date"),
    )
)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

grouped_driver_standing_df = race_result_df.groupBy(
    "race_year",
    "driver_name",
    "driver_nationality",
    "constructor_name",
).agg(
    sum("points").alias("points"),
    count(when(col("position") == "1",True)).alias("wins")
    )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

window = Window.partitionBy("race_year").orderBy(desc("points"),desc("wins"))

final_df = grouped_driver_standing_df.withColumn("rank", rank().over(window))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")


# Write to Managed Table
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
