#!/bin/bash
# Setup and initialize Apache Airflow for the Sparkify project

export AIRFLOW_HOME=~/airflow

# Install Airflow with required providers
pip install apache-airflow==2.7.0 \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.0/constraints-3.8.txt"

# Initialize the Airflow database
airflow db init

# Create default admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@sparkify.com \
    --password admin

# Copy DAGs and plugins into Airflow home
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/plugins"

cp -r dags/* "$AIRFLOW_HOME/dags/"
cp -r plugins/* "$AIRFLOW_HOME/plugins/"

echo "Airflow setup complete. Start the webserver and scheduler with:"
echo "  airflow webserver --port 8080 &"
echo "  airflow scheduler &"
