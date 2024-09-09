-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl7/processed"

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_processed;
