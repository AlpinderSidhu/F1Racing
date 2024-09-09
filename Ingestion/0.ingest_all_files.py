# Databricks notebook source
files = [
    "ingest_circuit_file",
    "ingest_constructors_file",
    "ingest_drivers_file",
    "ingest_laptimes_file",
    "ingest_pitstops_file",
    "ingest_qualifying_file",
    "ingest_races_file",
    "ingest_result_file",
]

data_source = "Ergast_API"

for file in files:
    print("Running " + file)
    status = dbutils.notebook.run(file, 1200, {"p_data_source": data_source})
    if status != "Success":
        print("Failed to run " + file)
        break
    print("Completed " + file)
