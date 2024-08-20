# Formula1 Racing

- Project overview

- Formula 1 data sources and datasets

http://ergast.com/mrd


http://ergast.com/mrd/terms/

### Project Requirements
#### Functional Requirements
##### Data Ingestion Requirements
- Ingest all fukes into the datafiles
- Ingested data must have the schema applied
- Ingested data must have audit column i.e date column or source column
- Ingested data must be stored in the columnar format i.e. parquet
- Must be able to to analyze the ingested data via SQL
- The data ingested should be available for all kind of workloads such as machine learning, further transformation for reporting and also analytical workloads via SQL.
- Ingestion logic must be able to handle the increment load

##### Data Transformation Requirements
- Join the key information required for **Reporting** to create a new table
- Join the key information required for **Analysis** to create a new table
- Transformed data must have audit columns
- Must be able to analyze the transformed data using SQL
- Tansformation logic should also be able to handle incremental data, similar to the ingestion requirements.

##### Reporting Requirements
Below reporting requirement is required for every year
- Driver Standings
- Constructor Standings 

##### Analysis Requirements
- Find most Dominant Driver over the last decade
- Find most Dominant Teams over the last decade
- Visualize the output
- Create Databricks Dashboards

#### Non-Functional Requirements
##### Scheduling Requirements
- Pipeline schedule to run every Sunday 10PM
- Ability to monitor the status of the pipeline
- Ability to re-run failed pipelines
- Ability to setup alerts on failure

##### Other Non-Functional Requirements
- Ability to delete individual records
- Ability to see the history and time travel


### Solution Architecture

**Ergast API** ----> **ADLS Raw Layer** --_Ingest_--> **ADLS Ingested layer** --_Transform_-->  **ADLS Presentation Layer** --_Analyze_--> Dashboards
                                                                                                                                  |
                                                                                                                                  |
                                                                                                                                  |--_Report_--> Power BI
- Ability to roll back to a previous version
