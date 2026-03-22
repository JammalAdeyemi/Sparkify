# Sparkify Airflow Project

This project builds a reusable Apache Airflow ETL pipeline for Sparkify data loaded from S3 into Amazon Redshift.

The pipeline stages raw event/song JSON files, loads a fact table and dimension tables, and runs data quality checks after transformations.

## Project Goals

- Build a dynamic DAG with reusable custom operators
- Stage JSON data from S3 to Redshift
- Load fact and dimension tables
- Run SQL-based data quality checks
- Support retry behavior and backfill-friendly task design

## Repository Structure

```text
Sparkify/
|-- DAGs/
|   `-- project_dag.py                 # Main Airflow DAG
|-- plugins/
|   |-- helpers/
|   |   `-- sql_queries.py             # SQL transformation statements
|   `-- operators/
|       |-- stage_redshift.py          # Stage from S3 -> Redshift
|       |-- load_fact.py               # Load fact table (append)
|       |-- load_dimension.py          # Load dimensions (truncate/append)
|       `-- data_quality.py            # Data quality checks
|-- images/
|   `-- final_project_dag_graph2.png   # DAG graph reference
|-- setup_airflow.sh                   # Local Airflow + environment setup
|-- set_aws_conn.sh                    # Airflow AWS/Redshift connections setup
|-- .gitignore
`-- README.md
```

## DAG Overview

DAG ID: `final_project_legacy`

Pipeline graph:

![Sparkify DAG Pipeline](images/final_project_dag_graph2.png)

Task flow:

1. `Begin_execution`
2. `Stage_events` and `Stage_songs` (in parallel)
3. `Load_songplays_fact_table`
4. `Load_artist_dim_table`, `Load_song_dim_table`, `Load_time_dim_table`, `Load_user_dim_table` (in parallel)
5. `Run_data_quality_checks`
6. `Stop_execution`

Default DAG behavior:

- `depends_on_past=False`
- `retries=3`
- `retry_delay=5 minutes`
- `catchup=False`
- `email_on_retry=False`

## Prerequisites

Before running, ensure you have:

- Python 3 available in your environment
- Access to an Amazon Redshift cluster
- S3 bucket/data available
- Airflow connection credentials available as environment variables in Codespaces (or your shell)

Expected environment variables for `set_aws_conn.sh`:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `REDSHIFT_HOST`
- `REDSHIFT_PORT`
- `REDSHIFT_DB`
- `REDSHIFT_USER`
- `REDSHIFT_PASSWORD`

## Setup and Run

Run from repository root.

### 1. Set up Airflow locally

```bash
bash setup_airflow.sh
```

This script will:

- Create a virtual environment at `~/airflow_venv`
- Install Airflow and providers
- Configure `AIRFLOW_HOME` as `/workspaces/Sparkify/airflow_home`
- Copy DAGs/plugins into Airflow folders
- Initialize Airflow database
- Create an admin user

### 2. Configure Airflow connections

```bash
source ~/airflow_venv/bin/activate
bash set_aws_conn.sh
```

This script will:

- Create or replace `aws_credentials`
- Create or replace `redshift`
- Set Airflow variables `s3_bucket` and `s3_prefix`

### 3. Start Airflow services

Terminal 1:

```bash
source ~/airflow_venv/bin/activate
airflow webserver --port 8080 --host 0.0.0.0
```

Terminal 2:

```bash
source ~/airflow_venv/bin/activate
airflow scheduler
```

Open Airflow UI on port `8080` from the Codespaces PORTS tab.

### 4. Validate DAG loading

```bash
source ~/airflow_venv/bin/activate
airflow dags list
airflow dags list-import-errors
```

### 5. Trigger the pipeline

From UI: trigger `final_project_legacy`, or via CLI:

```bash
source ~/airflow_venv/bin/activate
airflow dags trigger final_project_legacy
```

## Operators Summary

- `StageToRedshiftOperator`
	- Loads JSON from S3 to Redshift staging table via `COPY`
	- Supports templated `s3_key` for execution-date based paths

- `LoadFactOperator`
	- Appends transformed data into fact table

- `LoadDimensionOperator`
	- Loads dimensions using `truncate-insert` or append mode

- `DataQualityOperator`
	- Runs one or more SQL checks and compares results to expected values
	- Raises failure when any check does not match

## Notes

- The repository ignores `airflow_home/` to avoid committing Airflow runtime artifacts.
- If a connection setup command fails, verify all environment variables are exported in your active shell.