-- Databricks notebook source
-- DBTITLE 1,Creating database
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Ciruits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE f1_raw.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (PATH "/mnt/formula1dl7/raw/circuits.csv", HEADER TRUE)


-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Races Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
) USING CSV 
OPTIONS ( PATH '/mnt/formula1dl7/raw/races.csv', HEADER TRUE )

-- COMMAND ----------

SELECT * FROM f1_raw.races ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructors Table
-- MAGIC - Simple Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE f1_raw.constructors (
  constructorId STRING,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (PATH "/mnt/formula1dl7/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Drivers Table
-- MAGIC - Simple Line JSON
-- MAGIC - Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
) USING JSON
OPTIONS (PATH "/mnt/formula1dl7/raw/drivers.json");

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Results Table
-- MAGIC - Simple Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points DOUBLE,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed DOUBLE,
  statusId INT)
  USING JSON
  OPTIONS (PATH "/mnt/formula1dl7/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Pitstops Table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS (PATH "/mnt/formula1dl7/raw/pit_stops.json", multiLine TRUE)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create LapTimes Table
-- MAGIC - CSV File
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
) 
USING CSV
OPTIONS (
  PATH "/mnt/formula1dl7/raw/lap_times/*.csv",
  multiLine TRUE
)

-- COMMAND ----------

SELECT count(1) FROM f1_raw.lap_times; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Qualifying Table
-- MAGIC - JSON file
-- MAGIC - Multiple JSON
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
) USING JSON
OPTIONS (
  PATH "/mnt/formula1dl7/raw/qualifying/*.json",
  multiLine TRUE
)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;
