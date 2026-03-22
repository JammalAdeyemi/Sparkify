#!/bin/bash

source ~/airflow_venv/bin/activate
export AIRFLOW_HOME="/workspaces/Sparkify/airflow_home"

echo "Setting up AWS + Redshift connection..."

# Delete existing connection
airflow connections delete aws_credentials 2>/dev/null || true
airflow connections delete redshift 2>/dev/null || true

# Create AWS connection using URI
airflow connections add aws_credentials \
    --conn-uri "aws://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@"

# Create Redshift connection using URI
airflow connections add redshift \
    --conn-uri "redshift://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}"

echo "✓ aws_credentials connection created & variables set"