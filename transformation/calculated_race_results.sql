-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Results -> Drivers -> Constructors -> Races

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING PARQUET
AS
SELECT
  races.race_year,
  constructors.name as team_name,
  drivers.name as driver_name,
  results.position,
  results.points,
  11 - results.position as calculated_points
FROM results
JOIN drivers
ON (results.driver_id = drivers.driver_id)
JOIN constructors
ON (results.constructor_id = constructors.constructor_id)
JOIN races
ON (results.race_id = races.race_id)
Where results.position <= 10;
