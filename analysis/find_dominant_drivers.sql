-- Databricks notebook source
-- DBTITLE 1,Dominant Drivers
select 
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results 
group by driver_name
having total_races > 50
order by avg(calculated_points) desc
limit 10;

-- COMMAND ----------

-- DBTITLE 1,Decade 2011 - 2020
select
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results 
where race_year between 2011 and 2020
group by driver_name
having total_races > 50
order by avg(calculated_points) desc
limit 10;

-- COMMAND ----------

-- DBTITLE 1,Decade 2001 - 2010
select
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results 
where race_year between 2001 and 2010
group by driver_name
having total_races > 50
order by avg(calculated_points) desc
limit 10;
