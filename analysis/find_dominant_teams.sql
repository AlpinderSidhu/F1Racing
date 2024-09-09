-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results limit 10;

-- COMMAND ----------

-- DBTITLE 1,Dominant Races
SELECT 
  team_name,
  count(1) as total_races,
  sum(calculated_points),
  avg(calculated_points)
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races > 100
ORDER BY avg(calculated_points) DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Decade 2011-2020
SELECT 
  team_name,
  count(1) as total_races,
  sum(calculated_points),
  avg(calculated_points)
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
GROUP BY team_name
HAVING total_races > 100
ORDER BY avg(calculated_points) DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Decade 2001-2011
SELECT 
  team_name,
  count(1) as total_races,
  sum(calculated_points),
  avg(calculated_points)
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 and 2011
GROUP BY team_name
HAVING total_races > 100
ORDER BY avg(calculated_points) DESC
LIMIT 10;
